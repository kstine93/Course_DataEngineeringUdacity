## Linux Postgres Setup
- [Postgres setup help](how-to-install-and-use-postgresql-on-ubuntu-18-04)

### Install
`sudo apt install postgresql`

### Restarting Postgres
`sudo service postgresql restart`

---
## Starting / Stopping Server
### Start Postgres Server
`sudo systemctl start postgresql.service`

### Stop Postgres Server
`sudo systemctl stop postgresql.service`

---
## Enabling / Disabling Server
*Note: I don't really know what this means yet - and how it differs from start/stop*

### Enable Postgres Server
`sudo systemctl enable postgresql.service`

### Disable Postgres Server
`sudo systemctl disable postgresql.service`

---
## Using Postgres Server

### "Switch over" to postgres account
*Note: Uses my linux login information?*
`sudo -i -u postgres`

### Accessing Postgres CLI
postgres@server:~$`psql`

### (alternative) Access Postgres CLI from terminal:
`sudo -u postgres psql`

### Exit Postgres CLI
postgres=#`\q`

---
## Create new Postgres Role(s)

### Create new user from Postgres account:
postgres@server:~$ `createuser --interactive`

### (alternative) Create new user from terminal
`sudo -u postgres createuser --interactive`

*Note: "Superusers" have lots of access. Don't grant new roles this status without careful consideration*

### Grant access to specific schemas or tables
`GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO basic_user;`

`GRANT ALL PRIVILEGES ON DATABASE pagila TO basic_user;`

## Create new Database
postgres@server:~$ `createdb testdb`

## (alternative) Create new Database from terminal
`sudo -u postgres createdb testdb`


---
## General
### Get manual for command
postgres@server:~$ `man createuser`

### List users & permissions
postgres=#`\du`

### Force close connections:
postgres=#
```
SELECT
	pg_terminate_backend(pg_stat_activity.pid)
FROM
	pg_stat_activity
WHERE
	pg_stat_activity.datname = 'database_name'
	AND pid <> pg_backend_pid();
```