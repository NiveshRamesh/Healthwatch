pipeline {
    agent any

    stages {

        stage('Build Docker Image') {
            steps {
                sh 'docker build -t healthwatch .'
            }
        }

        stage('Save Image TAR') {
            steps {
                sh 'docker save healthwatch > healthwatch.tar'
            }
        }

    }
}