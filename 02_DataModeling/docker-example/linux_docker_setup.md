## Linux Docker Setup
- [Docker setup help](https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository)
- [Linux Post-install instructions](https://docs.docker.com/engine/install/linux-postinstall/)

## Configuring Docker to start on boot (or not)
### Docker start on boot
`sudo systemctl enable docker.service`

`sudo systemctl enable containerd.service`

### Docker do NOT start on boot
`sudo systemctl disable docker.service`

`sudo systemctl disable containerd.service`

---

## Using Docker
- [Getting started with Docker](https://docs.docker.com/get-started/)

### IMPORTANT
Docker requires root privileges `sudo` to run. So unless you want to create another user group (see post-install instructions; has some security drawbacks), you will need to preface all docker commands with `sudo`.
