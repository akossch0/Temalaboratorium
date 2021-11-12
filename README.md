# Overview

This application is for the purpose of providing an alternative solution to the problem of heavy loads during exam applicaitons in Neptun using Kafka messaging-system, CQS pattern and Event-sourcing. The application shows a primitive scenario in which Students apply for exams, these applications are handled and processed by the Command model, which then produces messages on to the Kafka Event Log about the succesful applications. Based on the Event Log, the Query model represents the applications in a certain way.

