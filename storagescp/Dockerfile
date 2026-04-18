FROM amazoncorretto:21.0.5-al2023
LABEL org.opencontainers.image.authors="Gunter Zeilinger <gunterze@protonmail.com>"

RUN set -eux \
    && yum install -y tar gzip \
    && yum clean all

ENV DCM4CHE_VERSION=5.33.1

RUN cd /opt \
    && curl https://www.dcm4che.org/maven2/org/dcm4che/dcm4che-assembly/${DCM4CHE_VERSION}/dcm4che-assembly-${DCM4CHE_VERSION}-bin.tar.gz | tar xz \
    && mv /opt/dcm4che-${DCM4CHE_VERSION} /opt/dcm4che

ENV PATH=/opt/dcm4che/bin:$PATH

RUN dnf install -y python3.11
RUN dnf install -y python3.11-pip

RUN ls -las /usr/bin
RUN python3.11 -m pip --version
RUN python3.11 -m pip install pydicom pynetdicom watchdog

RUN mkdir /opt/storagescp
RUN mkdir /opt/storagescp/incoming /opt/storagescp/outgoing /opt/storagescp/conversion
COPY storagescp.py /opt/storagescp/storagescp.py

ENTRYPOINT ["python3.11", "/opt/storagescp/storagescp.py"]

