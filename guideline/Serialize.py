# Databricks notebook source
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("My Spark App").setMaster("local").serializerSerializer("CloudPickleSerializer")