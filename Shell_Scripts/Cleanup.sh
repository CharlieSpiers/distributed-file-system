#!/bin/bash
# Recompiles all java classes for use in development
rm *.class
javac Logger.java
javac ConnectionThread.java
javac Controller.java
javac Dstore.java