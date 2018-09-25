# kafka-data-ingest

## Building 

This is a scala project build with SBT.

To start out install _SBT_ and _Java_ (At the moment we are running Java 9).

on mac:
`brew install sbt`

Java can be downloaded here: 
https://www.azul.com/downloads/zulu/

Now it should be possible to compile:
```
sbt compile
```

A runnable jar file can be created with:
```
sbt assembly
```

And there is a script to package everything into a tar tgz:
```
./package.sh
```

The main class that is started is `com.humio.ingest.main.Runner`

That is also a good place to dig into the code
