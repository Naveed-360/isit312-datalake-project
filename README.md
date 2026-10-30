# ISIT312 – Alpha Group Data Lake

One Docker setup that every member runs at home: HDFS, Hive, Spark and Kafka.

## Start it

Needs Docker Desktop with **at least 8 GB of memory** (Settings → Resources).

```bash
git clone <repo-url> && cd isit312-datalake
docker compose up -d
docker compose ps        # all 8 containers should say "running"
```

The first start downloads about 3 GB of images. Hive takes 1–2 minutes after that.

| What | Where |
|---|---|
| HDFS web UI | http://localhost:50070 |
| Spark web UI | http://localhost:8080 |
| Hive (SQL) | `docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000` |
| Kafka from your laptop | `localhost:29092` |
| Kafka from inside Docker | `kafka:9092` |

The `data/` and `jobs/` folders on your laptop appear as `/data` and `/jobs` inside the containers.

Stop with `docker compose down`. Your HDFS data stays. `docker compose down -v` wipes it.

**Apple Silicon Macs:** the Hadoop/Hive/Spark images are Intel-only and run under emulation.
If they fail, turn on *"Use Rosetta"* in Docker Desktop settings.

## If something breaks

| Problem | Fix |
|---|---|
| A container keeps restarting | `docker compose logs <name>`. Usually not enough memory: raise it to 8 GB. |
| `Connection refused` to Hive | Wait 2 minutes after starting, then try again. |
| Port already in use | Close the program using 8080/50070/10000, or change the left-hand port in `docker-compose.yml`. |
