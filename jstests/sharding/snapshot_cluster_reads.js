/**
 * Test readConcern: level: snapshot in a sharded cluster.
 */
(function() {
    "use strict"

        function runSnapshotReads(mainConn, priConn) {
            var lsid = {id: UUID()};
            var testDb = mainConn.getDB('test_snapshot');

            // Insert some data to query for
            let insertRes = testDb.coll.insert([{a: 1}]);
            assert.writeOK(insertRes);
            // TODO: change to lastCommittedOpTime when implemented
            // const lastCommittedOpTime = insertRes.operationTime;

            // Only the update statements with multi=true in a batch fail.
            var cmd = {
                aggregate: 'coll',
                pipeline: [{$match: {a: 1}}],
                cursor: {batchSize: 1},
                readConcern: {level: 'snapshot'},
                lsid: lsid,
                txnNumber: NumberLong(1)
            };
            var res = testDb.runCommand(cmd);
            assert.commandWorked(res);

            print("XXX res: " + tojson(res));
        }

    // TODO: add nodes to rs and shards later
    const st = new ShardingTest({shards: 1, mongos: 1, rs: {nodes: 1}});
    runSnapshotReads(st.s0, st.rs0.getPrimary());
    st.stop();
})()
