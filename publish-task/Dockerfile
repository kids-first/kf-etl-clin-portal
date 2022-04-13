FROM openjdk:11

COPY target/scala-2.12/publish-task.jar .

ENTRYPOINT ["java", "-jar", "publish-task.jar"]