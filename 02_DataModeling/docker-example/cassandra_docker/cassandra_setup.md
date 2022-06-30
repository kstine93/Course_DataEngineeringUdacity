## Cassandra Docker Image Setup
Following these instructions (and adapting them for Cassandra): https://docs.docker.com/language/python/develop/

### Creating volumes
Volumes are apparently ways to save information persistently with Docker: https://docs.docker.com/storage/volumes/

`sudo docker volume create cassandra`

`sudo docker volume create cassandra_config`


### Creating network
This apparently allows our applications to talk to each other via a private network.

`sudo docker network create cassandra_net`

### Run Cassandra in container, using created volumes + network

```
sudo docker run --rm -d -v cassandra:/var/lib/cassandra \
-v cassandra_config:/etc/cassandra -p 3306:3306 \
--network cassandra_net \
--name cassandra_db \
-e CASSANDRA_ROOT_PASSWORD="password" \
cassandra
```
(Note: I have no idea if the 'root password' is named correctly for cassandra - it might only be relevant for mysql)

---
NEXT STEPS:
I think the example I was using was a little to focused on mysql. Didn't really give me the tools to adapt to use Cassandra.
However, I can quickly get started with Cassandra by using a custom tutorial. Try this one and see where that gets you:
https://cassandra.apache.org/_/quickstart.html