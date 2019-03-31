

pipeline {
    options {
      timeout(time: 2, unit: 'HOURS') 
      skipDefaultCheckout()
  }
  agent {
    docker {
      image 'premhashmap/cain-buildenv:latest'
      args '-u root -v /var/run/docker.sock:/var/run/docker.sock'
    }
  }
  stages {
    stage('Going') {
      steps {  
      withCredentials(bindings: [usernamePassword(credentialsId: 'dockerhubPWD', usernameVariable: 'USERNAME', passwordVariable: 'PASSWORD')]) {
          sh '''
          sudo docker login -u $USERNAME -p $PASSWORD
          mkdir -p $GOPATH/src/github.com/prem0132 && cd $GOPATH/src/github.com/prem0132
          git clone https://github.com/prem0132/cain.git && cd cain
          make
          '''
        }
      }    
    }   
  }
  post {
    always {
      sh 'chmod -R 777 .'
    }
  }
}