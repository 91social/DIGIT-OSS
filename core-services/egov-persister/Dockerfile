
FROM egovio/alpine-maven-builder-jdk-8:1-master-NA-6036091e AS build  
WORKDIR /app

# copy the project files
COPY pom.xml ./pom.xml
COPY start.sh ./start.sh
COPY src/main/resources ./resources/

# not useful for stateless builds
# RUN mvn -B dependency:go-offline
COPY src ./src
RUN mvn -B -f /app/pom.xml package

# Create runtime image
FROM egovio/8-openjdk-alpine

WORKDIR /opt/egov
COPY --from=build /app/target/*.jar /app/start.sh /opt/egov/
COPY --from=build /app/resources resources/

RUN chmod +x /opt/egov/start.sh
CMD ["/opt/egov/start.sh"]
