## 本地非标准maven依赖库安装（解决无法传递依赖问题）

## --------------------------------------------------------------------------------install
### mvn install:install-file  -Dfile=elasticsearch-sql-5.6.16.jar  -DgroupId=org.nlpcn  -DartifactId=elasticsearch-sql  -Dversion=5.6.16  -Dpackaging=jar


## --------------------------------------------------------------------------------deploy
### mvn deploy:deploy-file    -Dfile=elasticsearch-sql-5.6.16.jar  -DgroupId=org.nlpcn  -DartifactId=elasticsearch-sql  -Dversion=5.6.16  -Dpackaging=jar  -Durl=http://maven.aliyun.com/nexus/content/repositories/releases/  -DrepositoryId=aliyun-nexus-releases
