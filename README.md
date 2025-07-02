# icestream
The Table-ator-inator is the easiest way to cheaply ingest data into your lakehouse in the whole Tri-State Area.

### Useful commands

```shell
docker run --name icestream-postgres -e POSTGRES_USER=icestream -e POSTGRES_PASSWORD=icestream -e POSTGRES_DB=icestream_dev -p 5432:5432 -d postgres:17
```
