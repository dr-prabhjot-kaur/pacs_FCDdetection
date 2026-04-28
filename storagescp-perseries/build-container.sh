

DOCKER_BUILDKIT=1 docker build --network=host \
  --progress=plain \
   -t tools -f Dockerfile .


