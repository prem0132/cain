

pipeline {
    options {
      timeout(time: 2, unit: 'HOURS') 
  }
  agent {
    docker {
      image 'golang'
      args '-u root -v /var/run/docker.sock:/var/run/docker.sock'
    }
  }
  stages {
    stage('Going') {
      steps {  
        sh '''
          make all
    '''
        }
    }       
  }
  post {
    always {
      sh 'chmod -R 777 .'
    }
  }
}