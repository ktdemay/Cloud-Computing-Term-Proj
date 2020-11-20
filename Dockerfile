FROM openjdk:7
COPY /src /usr/src/Cloud-Computing-Term-Proj
WORKDIR /usr/src/Cloud-Computing-Term-Proj
RUN javac term_proj.java
CMD ["java", "term_proj"]