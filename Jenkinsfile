pipeline {
    agent any

    stages {

        stage('Build Docker Image') {
            steps {
                bat 'docker build -t healthwatch .'
            }
        }

        stage('Save Image TAR') {
            steps {
                bat 'docker save healthwatch -o healthwatch.tar'
            }
        }

        stage('Copy Image to Server') {
            steps {
                bat 'scp healthwatch.tar ubuntu@103.65.21.198:/tmp/'
            }
        }

    }
}