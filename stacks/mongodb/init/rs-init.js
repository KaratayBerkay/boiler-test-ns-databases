// Idempotent replica-set bootstrap. Members are advertised by the names/ports that resolve both inside the compose
// network (mongoN) and, via /etc/hosts on the host, for a replicaSet-aware client (see lab.yaml notes).
const cfg = {
  _id: "rs0",
  members: [
    { _id: 0, host: "mongo1:27017", priority: 3 },
    { _id: 1, host: "mongo2:27018", priority: 2 },
    { _id: 2, host: "mongo3:27019", priority: 1 },
  ],
};
try {
  const st = rs.status();
  if (st.ok) { print("replica set already initiated: " + st.set); quit(0); }
} catch (e) { /* not initiated */ }
printjson(rs.initiate(cfg));
// wait for a primary
for (let i = 0; i < 60; i++) {
  try { if (rs.isMaster().ismaster || db.hello().isWritablePrimary) { print("primary elected"); quit(0); } } catch (e) {}
  sleep(1000);
}
print("no primary after 60 s"); quit(1);
