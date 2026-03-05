pipeline {
    agent any

    stages {

        stage('Clone Repo') {
            steps {
                git 'https://github.com/NiveshRamesh/Healthwatch.git'
            }
        }

        stage('Build Docker Image') {
            steps {
                sh 'docker build -t healthwatch .'
            }
        }

        stage('Save Image as TAR') {
            steps {
                sh 'docker save healthwatch > healthwatch.tar'
            }
        }
    }
}
