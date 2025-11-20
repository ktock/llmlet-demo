function newPeer(options) {
    var peer = new Peer(options && options.peerOptions || {});
    var connCB;
    peer.on('connection', function(conn){
        if (connCB != null) connCB(conn);
    });
    peer.on('error', function(err){
        console.error("peer connection error:" + err);
        if (options && options.printLog) options.printLog(err);
    });
    peer.on('open', options.onOpen);
    peer.on('disconnected', () => {
        console.log("Disconnected. Trying reconnect");
        peer.reconnect();
    });
    return {
        peer: peer,
        onConnection: (cb) => {connCB = cb;},
        printLog: options && options.printLog || ((t) => console.log(t)),
    };
}

function newPeerManager(Module, peer, options) {
    var pm = {
        peer: peer.peer,
        peerConnectionSet: {},
        peerConnectionSetNextId: 0,
        readyConns: [],
        readyConnsNotifier: [],
        closedConnsCache: {},
        connectionTimeout: options && options.connectionTimeout || 10000,
        allowedPeers: options && options.allowedPeers || (() => false),
    };

    const PEER_ID_MAX = 1024;

    function newID() {
        const start = pm.peerConnectionSetNextId;
        while (true) {
            if (pm.peerConnectionSetNextId >= PEER_ID_MAX) {
                pm.peerConnectionSetNextId = 0;
            }
            const id = pm.peerConnectionSetNextId++;
            if (!pm.peerConnectionSet[id]) {
                return id;
            }
            if (pm.peerConnectionSetNextId == start) {
                console.error("too many connections");
                return null;
            }
        }
    }

    function newPeerConnection(id, conn) {
        var pc = {
            id: id,
            conn: conn,
            ready: false,
            accepted: false,
            recvNotifier: [],
            dataMode: false,
            reset: () => {
                if (pc.module_buf != null) {
                    Module.release_conn(pc.module_buf);
                }
                pc.id = -1;
                pc.ready = false;
                pc.accepted = false;
                pc.recvBuf = null;
                pc.module_buf = null;
                for (const k in pc.recvNotifier) {
                    pc.recvNotifier[k]();
                }
                pc.recvNotifier = [];
                pc.dataMode = false;
            }
        };
        return pc;
    }

    function receiveChannelCB(pc, data) {
        if (pc.dataMode) {
            pc.dataMode = false;
            if ((pc.id >= 0) && (data.byteLength != 0)) {
                var id = pc.id;
                if (pc.recvBuf == null) {
                    pc.recvBuf = [];
                }
                pc.recvBuf.push(data);
                if (pc.recvNotifier.length != 0) {
                    while (true) {
                        var cb = pc.recvNotifier.shift();
                        if (cb == null) break;
                        if (cb()) {
                            break;
                        }
                    }
                }
            }
            return;
        }
        if (data.byteLength == 0) {
            return;
        }
        switch (data[0]) {
        case 0:
            if (pc.id < 0) {
                break;
            }
            pc.dataMode = true;
            break;
        case 1: // close
            if (pm.peerConnectionSet[pc.id] != null) {
                delete pm.peerConnectionSet[pc.id];
                pc.reset();
                console.log("closed", pc.id);
            }
            break;
        case 2: // open
            var newid = newID();
            if (newid == null) {
                break;
            }
            pc.id = newid;
            pc.ready = true;
            pm.peerConnectionSet[newid] = pc;
            if (pm.readyConnsNotifier.length == 0) {
                pm.readyConns.push(newid);
            } else {
                // reset state
                if (!sendData(pc.conn, new Uint8Array(0))) {
                    break;
                }
                // signal "accept"
                if (!sendData(pc.conn, new Uint8Array([4]))) {
                    break;
                }
                pc.accepted = true;
                var cb = pm.readyConnsNotifier.shift();
                cb(newid);
            }
            console.log("opened", newid);
            // reset state
            if (!sendData(pc.conn, new Uint8Array(0))) {
                break;
            }
            // signal "opened"
            if (!sendData(pc.conn, new Uint8Array([3]))) {
                break;
            }
            console.log("replied ready");
            break;
        case 3: // opened
            pc.ready = true;
            console.log("confirmed ready", pc.id);
            break;
        case 4: // accepted
            pc.accepted = true;
            console.log("accepted", pc.id);
            break;
        default:
            peer.printLog("unknown peer message");
        }
    }

    function is_conn_open(conn) {
        return (conn.dataChannel != null) &&
            (conn.dataChannel.readyState == 'open') &&
            (conn.peerConnection != null) &&
            (conn.peerConnection.connectionState == 'connected');
    }

    function sendData(conn, data) {
        if (!is_conn_open(conn)) {
            return false;
        }
        conn.send(data);
        return true;
    }

    function is_alive(id) {
        var pc = pm.peerConnectionSet[id];
        if (pc == null) {
            return false;
        }
        if (!is_conn_open(pc.conn)) {
            return false;
        }
        if ((!pc.ready) && ((pc.recvBuf == null) || ((pc.recvBuf.length == 0)))) {
            return false;
        }
        if (pc.id != id) {
            return false;
        }
        return true;
    }

    if (options && options.listen) {
        peer.onConnection(function(conn){
            console.log("received connection:", conn.peer);
            if (!pm.allowedPeers(conn.peer)) {
                console.error("rejecting connection from unexpected peer:" + conn.peer);
                conn.close(); // reject connection from unexpected peer
                return;
            }
            conn.on('open', function(){
                const id = newID();
                if (id == null) {
                    conn.close();
                    return;
                }
                var pc = newPeerConnection(id, conn);
                console.log("new connection from ", conn.peer);
                pc.ready = true;
                if (!conn.reliable) {
                    console.error("failed to establish the connection with realiable mode");
                    conn.close();
                    return
                }
                peer.printLog("new connection opened from " + conn.peer);
                conn.on('data', function(data) {
                    receiveChannelCB(pc, new Uint8Array(data));
                });
                conn.on('close', function(){
                    console.log("peer closed connection " + pc.id)
                    if (pm.peerConnectionSet[pc.id] != null) {
                        delete pm.peerConnectionSet[pc.id];
                    }
                    pc.reset();
                })
                pm.peerConnectionSet[pc.id] = pc;
                if (pm.readyConnsNotifier.length == 0) {
                    pm.readyConns.push(pc.id);
                } else {
                    // reset state
                    if (!sendData(pc.conn, new Uint8Array(0))) {
                        return;
                    }
                    // signal "accept"
                    if (!sendData(pc.conn, new Uint8Array([4]))) {
                        return;
                    }
                    pc.accepted = true;
                    var cb = pm.readyConnsNotifier.shift();
                    cb(pc.id);
                }
            });
        });
    }

    pm.register_buf = (id, buf) => {
        if (pm.peerConnectionSet[id] != null) {
            pm.peerConnectionSet[id].module_buf = buf;
        }
    }
    
    pm.accept = (notify) => {
        if (pm.readyConns[0] == null) {
            pm.readyConnsNotifier.push(notify);
            return;
        }
        if (!is_alive(pm.readyConns[0])) {
            pm.readyConnsNotifier.push(notify);
            return;
        }
        var pc = pm.peerConnectionSet[pm.readyConns[0]];
        // reset state
        if (!sendData(pc.conn, new Uint8Array(0))) {
            return;
        }
        // signal "accept"
        if (!sendData(pc.conn, new Uint8Array([4]))) {
            pm.readyConnsNotifier.push(notify);
            return;
        }
        pc.accepted = true;
        notify(pm.readyConns.shift());
        return
    };

    pm.check = (id) => {
        var pc = pm.peerConnectionSet[id];
        if (!is_alive(id) || pc && !pc.accepted) {
            if (pc && ((Date.now() - pc.connectionStarted) > pm.connectionTimeout)) {
                console.error("connection timeout on " + id);
                return -1;
            }
            return 0; // not ready
        }
        return 1;
    };

    pm.connect = (dst) => {
        var id = newID();
        if (id == null) {
            return null;
        }
        if (pm.closedConnsCache[dst]) {
            var pc = pm.closedConnsCache[dst];
            delete pm.closedConnsCache[dst];
            if (is_conn_open(pc.conn)) {
                console.log("reusing conn", dst, "newid:", id);
                pc.id = id;
                pm.peerConnectionSet[pc.id] = pc;
                pc.connectionStarted = Date.now();
                pc.conn.send(new Uint8Array(0)); // reset state
                pc.conn.send(new Uint8Array([2])); // signal "open"
                return pc.id;
            }
        }
        var conn = pm.peer.connect(dst, {reliable: true});
        var pc = newPeerConnection(id, conn);
        pc.connectionStarted = Date.now();
        pm.peerConnectionSet[id] = pc;
        console.log("connecting to", dst);
        peer.printLog("connecting to " + dst);
        conn.on('open', function(){
            pc.ready = true;
            conn.on('data', function(data) {
                receiveChannelCB(pc, new Uint8Array(data));
            });
        });
        conn.on('close', function(){
            console.log("peer closed connection " + pc.id)
            if (pm.peerConnectionSet[pc.id] != null) {
                delete pm.peerConnectionSet[pc.id];
            }
            pc.reset();
        })
        return pc.id;
    };

    pm.send = (id, d) => {
        if (pm.peerConnectionSet[id] != null) {
            const chunkSize = 64 * 1024; // TODO: make it configurable
            var offset = 0;
            const conn = pm.peerConnectionSet[id].conn;
            if (!is_conn_open(conn)) {
                console.error("connection closed");
                return offset;
            }
            try {
                // reset state
                conn.send(new Uint8Array(0));
            } catch (e) {
                console.log("failed to reset state");
                return 0;
            }
            while (offset < d.byteLength) {
                const chunk = d.subarray(offset, offset + chunkSize);
                var len = chunkSize;
                if (len > (d.byteLength - offset)) {
                    len = d.byteLength - offset;
                }
                try {
                    // data mode
                    conn.send(new Uint8Array([0]));
                    conn.send(chunk);
                } catch (e) {
                    console.log("failed to send", e);
                    return offset;
                }
                offset += chunkSize;
            }
            return d.byteLength;
        } else {
            console.log("connection not found:", id);
        }
    };

    function readRecvBuf(pc, len, writeCB) {
        if ((pc.recvBuf == null) || (pc.recvBuf.length == 0)) {
            return 0;
        }
        var written = 0;
        while (pc.recvBuf.length != 0) {
            if (len == 0) {
                break;
            }
            var elmlen = len;
            if (elmlen > pc.recvBuf[0].byteLength) {
                elmlen = pc.recvBuf[0].byteLength;
            }
            writeCB(pc.recvBuf[0].slice(0, elmlen));
            if (elmlen < pc.recvBuf[0].byteLength) {
                pc.recvBuf[0] = pc.recvBuf[0].slice(elmlen);
            } else {
                pc.recvBuf.shift();
            }
            written += elmlen;
            len -= elmlen;
        }
        return written;
    }

    pm.recv = (id, len, notifyCB) => {
        if (pm.peerConnectionSet[id] != null) {
            var pc = pm.peerConnectionSet[id];
            var written = readRecvBuf(pc, len, notifyCB.writeCB);
            if (written == 0) {
                pc.recvNotifier.push(() => {
                    if (notifyCB.done) return false;
                    readRecvBuf(pc, len, notifyCB.writeCB);
                    notifyCB.cb();
                    return true;
                });
                setTimeout(() => {
                    notifyCB.cb(null);
                }, 10000);
                return;
            }
            notifyCB.cb();
            return;
        } else {
            notifyCB.cb(null);
        }
        return
    };

    pm.close_connection = (id) => {
        if (pm.peerConnectionSet[id] != null) {
            console.log("close", id);
            // Cache the connection to avoid too frequent requests when
            // the C code does frequent close and open of the socket.
            var pc = pm.peerConnectionSet[id];
            delete pm.peerConnectionSet[id];
            pc.reset();

            if (pm.closedConnsCache[pc.conn.peer] != null) {
                pm.closedConnsCache[pc.conn.peer].conn.close();
            }
            pm.closedConnsCache[pc.conn.peer] = pc;

            // reset state
            if (!sendData(pc.conn, new Uint8Array(0))) {
                return;
            }
            sendData(pc.conn, new Uint8Array([1])); // signal "close"
        }
    };

    pm.close = () => {
        for (const id in pm.peerConnectionSet) {
            if (pm.peerConnectionSet[id] != null) {
                pm.peerConnectionSet[id].conn.close();
                delete pm.peerConnectionSet[id];
            }
        }
        for (const dst in pm.closedConnsCache) {
            if (pm.closedConnsCache[dst] != null) {
                pm.closedConnsCache[dst].conn.close();
                delete pm.closedConnsCache[dst];
            }
        }
        console.log("============ closed peer manager ===========");
    }

    return pm;
}

const chunkMax = 100000000; //100MB

function addRemoteFile(Module, chunkcache, fileID, dir, fname, size, fetchFunc) {
    if (Module['preRun'] == null) {
        Module['preRun'] = [];
    }
    Module['preRun'].push((mod) => {
        mod.addRunDependency('load-model');
        (async () => {

            try {
                const FS = Module.FS;
                const fullpath = dir + '/' + fname;

                try { FS.mkdirTree(dir); } catch (e) {}
                FS.createDataFile(dir, fname, new Uint8Array(0), true, false, true);
                const node = FS.lookupPath(fullpath).node;
                if (!node) {
                    console.error('lookupPath did not return node; abort');
                    return;
                }

                node.size = size;

                const maxEntries = 5;
                node.waitingTable = {};
                node.remote_error = null;

                node.stream_ops = {
                    read(stream, buffer, offset, length, position) {
                        try {
                            if (length == 0) {
                                return 0;
                            }
                            if (node.remote_error != null) {
                                throw new FS.ErrnoError(28);
                            }

                            var idx = Math.floor(position / chunkMax);
                            var chunkPosition = idx * chunkMax;
                            var chunkSize = Math.min(chunkMax, node.size - chunkPosition);
                            
                            if (node.waitingTable[idx] != null) {
                                if (!node.waitingTable[idx].done) {
                                    throw new FS.ErrnoError(6);
                                }
                                const innerOfs = position % chunkMax;
                                const size = Math.min(length, node.waitingTable[idx].length - innerOfs);
                                buffer.set((node.waitingTable[idx].res.subarray(innerOfs, innerOfs + size)), offset);
                                node.waitingTable[idx].lastUsed = Date.now();

                                if (Object.keys(node.waitingTable).length > maxEntries) {
                                    var oldest = null;
                                    for (const k in node.waitingTable) {
                                        if (oldest == null) {
                                            oldest = k;
                                            continue;
                                        }
                                        if (node.waitingTable[k].lastUsed < node.waitingTable[oldest].lastUsed) {
                                            oldest = k;
                                        }
                                    }
                                    if (oldest != null) {
                                        node.waitingTable[oldest].res = null;
                                        delete node.waitingTable[oldest];
                                    }
                                }
                                return size;
                            }

                            node.waitingTable[idx] = { res: null, error: null };
                            const key = "chunk:" + fileID + ":" + chunkPosition + "-" + (chunkPosition + chunkSize);
                            chunkcache.get(key).then(data => {
                                if (data != null) {
                                    node.waitingTable[idx].res = new Uint8Array(data);
                                    node.waitingTable[idx].done = true;
                                    node.waitingTable[idx].length = data.byteLength;
                                    return;
                                }
                                fetchFunc(chunkPosition, chunkPosition + chunkSize).then(data => {
                                    chunkcache.put(key, data).then(() => {
                                        console.log("cached data: " + key);
                                    }).catch(error => {
                                        console.log("failed to cache data " + error);
                                    });
                                    node.waitingTable[idx].res = null;
                                    node.waitingTable[idx].res = new Uint8Array(data);
                                    node.waitingTable[idx].done = true;
                                    node.waitingTable[idx].length = data.byteLength;
                                }).catch(error => {
                                    console.error('Error:', error);
                                    node.remote_error = error;
                                });
                            }).catch(error => {
                                console.error("error from chunk cache: " + error);
                                node.remote_error = error;
                            });

                            throw new FS.ErrnoError(6);
                        } catch (e) {
                            if (!(e.name === "ErrnoError")) {
                                node.remote_error = e;
                                console.error("error during read", e);
                                throw new FS.ErrnoError(28); // EINVAL
                            }
                            throw e;
                        }
                    },

                    llseek(stream, offset, whence) {
                        let pos = offset;
                        if (whence === 1) pos += stream.position; // SEEK_CUR
                        else if (whence === 2) pos = node.size + offset; // SEEK_END
                        if (pos < 0) throw new FS.ErrnoError(22);
                        return pos;
                    },
                };
            } catch (err) {
                console.log("failed to getmodel" + err);
            } finally {
                mod.removeRunDependency('load-model');
            }
        })();
    });
}

async function chunkCache(quota) {
    if ((quota == null) || (quota == 0)) {
        const usageinfo = await navigator.storage.estimate();
        console.log(`usage: ${usageinfo.usage}, quota: ${usageinfo.quota}`);
        quota = usageinfo.quota / 2;
    }
    console.log("creating chunk cache with quota:", quota);
    
    const name = 'ChunkCache';
    const storeName = 'chunks';
    const db = await new Promise((resolve, reject) => {
        const req = indexedDB.open(name, 1);
        req.onupgradeneeded = () => {
            req.result.createObjectStore(storeName);
        };
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error);
    });

    var chunkCache = {
        db: db,
        storeName: storeName,
        quota: quota,
    };

    async function cleanupIfNeeded(chunksize) {
        const usageinfo = await navigator.storage.estimate();
        if (quota >= usageinfo.usage + chunksize) {
            return;
        }

        if (chunksize > quota) {
            throw new Error(`chunk is too large ${chunksize}`);
        }

        const keys = await new Promise((resolve, reject) => {
            const result = [];
            const tx = chunkCache.db.transaction(chunkCache.storeName, 'readonly');
            const store = tx.objectStore(chunkCache.storeName);

            const req = store.openCursor();
            req.onsuccess = e => {
                const cursor = e.target.result;
                if (cursor) {
                    result.push(cursor.key);
                    cursor.continue();
                } else {
                    resolve(result);
                }
            };
            req.onerror = () => reject(req.error);
        });

        console.log(`Found ${keys.length} keys to consider for cleanup.`);

        const freeTargetBytes = Math.max(quota / 2, chunksize);

        for (const key of keys) {

            // delete entry
            const tx = chunkCache.db.transaction(chunkCache.storeName, 'readwrite');
            tx.objectStore(storeName).delete(key);
            await new Promise((res, rej) => {
                tx.oncomplete = res;
                tx.onerror = () => rej(tx.error);
                tx.onabort = () => rej(tx.error);
            });

            // check quota
            const estimate = await navigator.storage.estimate();
            console.log(`Deleted key=${key}, usage=${estimate.usage}, quota=${quota}`);
            if (estimate.usage < freeTargetBytes) {
                console.log("cleanup complete: usage under target");
                return;
            }
        }

        const resinfo = await navigator.storage.estimate();
        if (quota < resinfo.usage + chunksize) {
            throw new Error(`failed to put to cache: no empty space (quota:${quota})`);
        }
    }

    chunkCache.put = async (key, chunk) => {
        try {
            await cleanupIfNeeded(chunk.byteLength);
        } catch (error) {
            throw new Error(`failed to put a chunk to the cache: ${error}`);
        }
        const tx = chunkCache.db.transaction(chunkCache.storeName, 'readwrite');
        const store = tx.objectStore(chunkCache.storeName);
        await new Promise((resolve, reject) => {
            const req = store.put(chunk, key);
            req.onsuccess = () => resolve();
            req.onerror = () => reject(req.error);
        });
        await tx.done;
    }

    chunkCache.get = async (key) => {
        const tx = chunkCache.db.transaction(chunkCache.storeName, 'readonly');
        const store = tx.objectStore(chunkCache.storeName);
        return new Promise((resolve, reject) => {
            const req = store.get(key);
            req.onsuccess = () => resolve(req.result);
            req.onerror = () => reject(req.error);
        });
    }

    return chunkCache;
}

async function fetchModel(fileID, modelURL, chunkcache) {
    const response = await fetch(modelURL);
    const reader = response.body.getReader();
    
    let chunk = new Uint8Array(chunkMax);
    let chunkN = 0;
    let chunkPosition = 0;
    while (true) {
        const { done, value } = await reader.read();
        if (done) {
            let key = "chunk:" + fileID + ":" + chunkPosition + "-" + (chunkPosition + chunkN);
            try {
                await chunkcache.put(key, chunk.subarray(0, chunkN));
            } catch(error) {
                console.error("failed to put the final blob to cache: " + error);
                return false;
            }
            break;
        }

        let nr = 0;
        while (nr < value.length) {
            var len = value.length - nr;

            if (len > (chunkMax - chunkN)) {
                len = chunkMax - chunkN;
            }
            chunk.set(value.subarray(nr, nr + len), chunkN);
            chunkN += len;
            if (chunkN == chunkMax) {
                let key = "chunk:" + fileID + ":" + chunkPosition + "-" + (chunkPosition + chunkMax);
                try {
                    await chunkcache.put(key, chunk);
                } catch(error) {
                    console.error("failed to put blob to cache: " + error);
                    return false;
                }
                chunkPosition += chunkMax;
                chunkN = 0;
            }
            
            nr += len;
        }
    }

    return true;
}

async function digestStr(str) {
    const data = new TextEncoder().encode(str);
    const hash = await crypto.subtle.digest('SHA-256', data);
    const hashArray = Array.from(new Uint8Array(hash));
    return hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
}

async function runClient(peer, module, Module, options) {
    const storageQuota = options.storageQuota;
    const chunkcache = await chunkCache(storageQuota);
    Module.ChunkCache = chunkcache;

    if (options.modelFile != null) {
        Module['arguments'].push('-m');
        Module['arguments'].push('/work/model.gguf');
        const file = options.modelFile;
        const filenameHash = await digestStr(file.name);
        addRemoteFile(Module, chunkcache, filenameHash, '/work', 'model.gguf', file.size, (b, e) => {
            return file.slice(b, e).arrayBuffer();
        });
    } else if (options.modelURL != '') {
        Module['arguments'].push('-m');
        Module['arguments'].push('/work/model.gguf');

        const modelURL = options.modelURL;
        let response = await fetch(modelURL, { method: 'HEAD' });
        if (!response.ok) {
            throw new Error(`failed to access to the model file: status: ${response.status}`);
        }
        const contentLength = response.headers.get('content-length');
        const size = Number(contentLength)

        const filenameHash = await digestStr(modelURL);

        let forceFullLoad = false;

        response = await fetch(modelURL, {headers: {'Range': 'bytes=0-1'}});
        if (response.status != 206) {
            console.error('server does not support range request. Trying full load');
            if (chunkcache.quota < size) {
                options.onExit(new Error("chunk cache does not have enough space for the model"));
                return;
            }
            forceFullLoad = true;
            options.output('HTTP Range Request is not supported by the server. Trying to fully load the model.\n');
            if (!(await fetchModel(filenameHash, modelURL, chunkcache))) {
                options.onExit(new Error("failed to load model"));
                return;
            }
        }

        addRemoteFile(Module, chunkcache, filenameHash, '/work', 'model.gguf', size, (b, e) => {
            if (forceFullLoad) {
                throw new Error("range fetch is unsupported on the full load mode");
            }
            return fetch(modelURL, {
                headers: {'Range': 'bytes=' + b + '-' + (e -1)}
            }).then(response => {
                if (response.status === 206) {
                    return response.blob();
                } else {
                    console.error(`Failed to fetch range: status: ${response.status}`);
                    throw new Error('Failed to fetch range');
                }
            }).then(data => {
                return data.arrayBuffer();
            });
        });
    } else {
        options.onExit(new Error("specify model to use"));
        return;
    }

    for (const i in options.args) {
        Module['arguments'].push(options.args[i]);
    }
    
    try {
        await module.default(Module);
    } catch (e) {
        console.log("failed to run client" + e);
        options.onExit(e);
    }

    return;
}

async function runServer(peer, module, Module, options) {
    const storageQuota = options.storageQuota;
    const chunkcache = await chunkCache(storageQuota);
    Module.ChunkCache = chunkcache;

    try {
        await module.default(Module);
    } catch (e) {
        console.log("failed to run server" + e);
        options.onExit(e);
    }
}

var lastServerStart = null;
var frequentServerCount = 0;
var frequentServerCountLim = 5;
function startServer(peer, module, options) {
    var Module = {};
    Module['print'] = (l) => options.output(l + '\n');
    Module['printErr'] = (l) => options.output(l + '\n');
    Module['stdin'] = () => null;

    Module.PeerManager = newPeerManager(Module, peer, {
        listen: true,
        allowedPeers: (p) => options.getTargetNodes().includes(p),
    });    

    var abortCalled = false;
    var serverRestarted = false;
    var exitHandler = (e) => {
        options.output('exited: ' + e + '\n');
        Module.PeerManager.close();
        if (!serverRestarted) {
            startServer(peer, module, options);
            serverRestarted = true;
        }
    }
    Module['onExit'] = exitHandler;
    Module['onAbort'] = () => {
        if (abortCalled) {
            return;
        }
        abortCalled = true;
        try {
            Module.PThread.terminateAllThreads();
            Module.PeerManager.close();
        } catch(e) {
            console.log(e);
        }
        if (!serverRestarted) {
            startServer(peer, module, options);
            serverRestarted = true;
        }
    }

    Module['preRun'] = [
        () => {
            Module.ENV.NO_COLOR = "1";
        },
    ];

    Module['arguments'] = [
        '-d',
        '-rpcbackend',
    ];

    if (lastServerStart != null) {
        if (Date.now() - lastServerStart < 1000) {
            frequentServerCount++;
        } else {
            frequentServerCount = 0;
        }
    }
    if (frequentServerCount >= frequentServerCountLim) {
        options.output('Restarting too frequently (' + frequentServerCount + 'times). Stopped server.\n');
        return;
    }
    lastServerStart = Date.now();
    runServer(
        peer,
        module,
        Module,
        {
            onExit: exitHandler,
            storageQuota: options.getStorageQuota && options.getStorageQuota() || 0,
        },
    );
    return {
        exit: () => {
            try {
                Module._emscripten_force_exit(0);
            } catch(e) {
                console.log(e);
            }
        },
    }
}

function startClient(peer, module, options) {
    var Module = {};
    Module['print'] = (l) => options.output(l + '\n');
    Module['printErr'] = (l) => options.outputErr(l + '\n');

    Module.PeerManager = newPeerManager(Module, peer);

    var running = true;
    
    var abortCalled = false;
    var exitHandler = (e) => {
        options.outputErr('exited: ' + e + '\n');
        running = false;
        Module.PeerManager.close();
    }
    Module['onExit'] = exitHandler;
    Module['onAbort'] = () => {
        running = false;
        if (abortCalled) {
            return;
        }
        abortCalled = true;
        try {
            Module.PThread.terminateAllThreads();
            Module.PeerManager.close();
        } catch(e) {
            console.log(e);
        }
    }

    Module['arguments'] = ['-d'];
    var peersList = options.getTargetNodes();
    for (const i in peersList) {
        if (peersList[i] == peer.peer.id) {
            continue;
        }
        Module['arguments'].push('-rpc');
        Module['arguments'].push(peersList[i]);
        console.log("Added node " + peersList[i]);
    }

    var inputBuf = "";
    var pending_prompt_reader = [];
    Module.pending_prompt = (cb) => {
        options.output("(you) ");
        if (inputBuf != "") {
            const res = inputBuf;
            inputBuf = "";
            options.output(res + '\n');
            cb(res);
            options.output("(output) ");
        } else {
            pending_prompt_reader.push((res) => {
                options.output(res + '\n');
                cb(res);
                options.output("(output) ");
            });
        }
    };

    Module['preRun'] = [
        () => {
            Module.ENV.NO_COLOR = "1";
        },
    ];

    runClient(
        peer,
        module,
        Module,
        {
            onExit: exitHandler,
            args: options.args,
            modelFile: options.getModelFile && options.getModelFile() || null,
            modelURL: options.getModelURL && options.getModelURL() || '',
            storageQuota: options.getStorageQuota && options.getStorageQuota() || 0,
        },
    );

    return {
        module: Module,
        exit: () => {
            try {
                Module._emscripten_force_exit(0);
            } catch(e) {
                console.log(e);
            }
        },
        isRunning: () => running,
        input: (msg) => {
            if (pending_prompt_reader.length == 0) {
                inputBuf = msg;
            } else {
                var cb = pending_prompt_reader.shift();
                cb(msg);
            }
        }
    };
}
