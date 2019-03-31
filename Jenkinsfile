

pipeline {
    options {
      timeout(time: 2, unit: 'HOURS') 
      skipDefaultCheckout()
  }
  agent {
    docker {
      image 'golang'
      args '-u root -v /var/run/docker.sock:/var/run/docker.sock -v /usr/bin/docker:/usr/bin/docker'
    }
  }
  stages {
    stage('Going') {
      steps {  
        sh '''
          mkdir -p $GOPATH/src/github.com/prem0132 && cd $GOPATH/src/github.com/prem0132
          git clone https://github.com/prem0132/cain.git && cd cain
          make
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