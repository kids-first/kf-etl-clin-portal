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
       pending("${env.JOB_NAME}","qa","${slackResponse.threadId}")
       sh '''
          ./deploy.sh qa
         '''
       success("${env.JOB_NAME}","qa","${slackResponse.threadId}")
     }
     post {
       failure {
         fail("${env.JOB_NAME}","qa","${slackResponse.threadId}")
       }
     }
   }

   stage('Approve deploying ETL in prd') {
      options {
        timeout(time: 1, unit: 'HOURS')
      }
      when {
        expression {
          return env.BRANCH_NAME == 'master';
        }
      }
      environment {
        APP_LEVEL = "prd"
      }
      steps {
        script {
            slackSend (color: '#FFFF00', channel: "${slackResponse.threadId}", message: ":sweat_smile: ${env.JOB_NAME} - ${env.BRANCH_NAME}[${env.BUILD_NUMBER}]: Stage ${STAGE_NAME} in ${env.APP_LEVEL} (${env.BUILD_URL}) <@UKGH3QN65>, <@U8A3WUF8D>", replyBroadcast: true)
            env.DEPLOY_TO_PRD = input message: 'User input required',
            submitter: "D3B_JENKINS_ADMINS",
            parameters: [choice(name: 'kf-etl-clin-portal: Deploy to PRD Environment', choices: 'no\nyes', description: 'Choose "yes" if you want to deploy the PRD server')]
        }
      }
      post {
        failure {
          script {
            slackSend (color: '#ff0000', channel: "${slackResponse.threadId}", message: ":x: ${env.JOB_NAME} - ${env.BRANCH_NAME}[${env.BUILD_NUMBER}]: Stage ${STAGE_NAME} in ${env.APP_LEVEL} (${env.BUILD_URL})")
          }
        }
      }
    }

   stage('Deploy prd'){
     when {
       expression {
         return env.BRANCH_NAME == 'master' && env.DEPLOY_TO_PRD == 'yes';
       }
     }
     steps{
       pending("${env.JOB_NAME}","prd","${slackResponse.threadId}")
       sh '''
          ./deploy.sh prd
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

