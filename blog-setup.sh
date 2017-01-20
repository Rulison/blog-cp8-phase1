#!/bin/bash

sudo apt-get update

# install java
sudo apt-get -y install default-jre

# install javac
sudo apt-get -y install default-jdk

# install python2.7
sudo apt-get -y install python-minimal

# one-time setup to read in training data and write blog model representation.
git clone https://github.com/BayesianLogic/blog
cd blog
sbt/sbt compile
sbt/sbt stage
cd ..
