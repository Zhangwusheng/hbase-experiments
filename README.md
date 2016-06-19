# hbase-experiments
fun with hbase

# how to run

- produce the jar

  ```
git clone git@github.com:sel-fish/hbase-experiments.git
cd hbase-experiments
mvn package -Dmaven.test.skip.exec=true
# after that, you got target/hbase-experiments-*.jar 
```

- copy ```target/hbase-experiments-*.jar``` to ```$HBASE_HOME/lib``` of all hbase nodes

- restart hbase

- run the test
  ```
cd hbase-experiments
mvn test
```
