#!groovy
properties([
    pipelineTriggers([[$class:"SCMTrigger", scmpoll_spec:"* * * * *"]])
])

pipeline {
  agent { label 'terraform-testing' }

  stages {
    stage('Get Code'){
      steps {
        deleteDir()
        checkout scm
        script {
          slackResponse = slackSend (color: '#FFFF00', message: "${env.JOB_NAME} :sweat_smile: Starting Jenkins pipeline: Branch '${env.BRANCH_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
        }
      }
    }
    stage('Build'){
      when {
        expression {
          return env.BRANCH_NAME == 'master';
        }
      }
      steps{
        pending("${env.JOB_NAME}","prd","${slackResponse.threadId}")
        sh '''
           ./build.sh
          '''
        success("${env.JOB_NAME}","prd","${slackResponse.threadId}")
      }
      post {
        failure {
          fail("${env.JOB_NAME}","prd","${slackResponse.threadId}")
        }
      }
    }
   stage('Deploy QA'){
     when {
       expression {
         return env.BRANCH_NAME == 'master';
       }
     }
     steps{
       pending("${env.JOB_NAME}","prd","${slackResponse.threadId}")
       sh '''
          ./deploy.sh qa
         '''
       success("${env.JOB_NAME}","prd","${slackResponse.threadId}")
     }
     post {
       failure {
         fail("${env.JOB_NAME}","prd","${slackResponse.threadId}")
       }
     }
   }

  }
}

void success(projectName,syslevel,channel="jenkins-kf") {
//   sendStatusToGitHub(projectName,"success")
  slackSend (color: '#00FF00', channel: "${channel}", message: "${projectName}:smile: Deployed to ${syslevel}: Branch '${env.BRANCH_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
}

void fail(projectName,syslevel,channel="jenkins-kf") {
//   sendStatusToGitHub(projectName,"failure")
  slackSend (color: '#ff0000', channel: "${channel}", message: "${projectName}:frowning: Deployed to ${syslevel} Failed: Branch '${env.BRANCH_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})", replyBroadcast: true)
}

void pending(projectName, syslevel,channel="jenkins-kf") {
  //sendStatusToGitHub(projectName, "pending")
  slackSend (color: '#FFFF00', channel: "${channel}", message: "${projectName}:sweat_smile:Starting to deploy to ${syslevel}: Branch '${env.BRANCH_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
}

