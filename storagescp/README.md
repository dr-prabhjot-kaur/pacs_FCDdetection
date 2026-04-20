# Purpose
This illustrates how to :
1. receive a DICOM connection request in an Application Entity over the network.
1. Save incoming images into a local directory.
1. Process the images when they have all been received and the transfer connection released.

# Notes on Docker out of Docker
A variety of different processing steps can be called from a container running on the processing host.
When the processing host is also a container, it is convenient to use Docker-out-of-Docker.

Docker-out-of-Docker (DooD)
In the DooD approach, only the Docker CLI runs in a container and connects to the Docker daemon on the host. The connection is done by mounting the host’s Docker’s socket into the container that runs the Docker CLI. For example:

$ docker run -it -v /var/run/docker.sock:/var/run/docker.sock docker
In this approach, containers created from within the Docker CLI container are actually sibling containers (spawned by the Docker daemon in the host). There is no Docker daemon inside a container and thus no container nesting. It has been nicknamed Docker-out-of-Docker (DooD).
