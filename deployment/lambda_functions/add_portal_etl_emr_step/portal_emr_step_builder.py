class EmrStepArgumentBuilder:
    def __init__(self):
        self.step_args = []

    def with_spark_job(self, class_name: str, etl_portal_bucket: str):
        self.step_args.extend([
            "spark-submit",
            "--deploy-mode",
            "client",
            "--class",
            f"{class_name}",
            f"s3a://{etl_portal_bucket}/jobs/etl.jar",
        ])
        return self

    def with_fhir_custom_job(self, etl_portal_bucket: str, release_id: str, studies: list, fhir_url: str):
        fhavro_export_args = [
            "aws", "s3", "cp",
            f"s3://{etl_portal_bucket}/jobs/fhavro-export.jar", "/home/hadoop;",
            f"export FHIR_URL='{fhir_url}'; export BUCKET='{etl_portal_bucket}';",
            "cd /home/hadoop;","/usr/lib/jvm/java-11-amazon-corretto.x86_64/bin/java -jar",
            f"fhavro-export.jar {release_id} {','.join(studies)} default y"
        ]

        self.step_args.extend([
            "bash",
            "-c",
            " ".join(fhavro_export_args)
        ])
        return self

    def with_packages(self, packages: list):
        packages.insert(0, "--packages")
        index = self.step_args.index("spark-submit")
        self.step_args[index + 1:index + 1] = packages
        return self

    def with_studies(self, studies: list, include_flag=False):
        if include_flag:
            self.step_args.append("--study-id")
        self.step_args.append(','.join(studies))
        return self

    def with_config(self, env: str, account: str, include_flag=False):
        if include_flag:
            self.step_args.append("--config")
        self.step_args.append(f"config/{env}-{account}.conf")
        return self

    def with_steps(self, step="default"):
        self.step_args.extend(["--steps", f"{step}"])
        return self

    def with_release_id(self, release_id: str, include_flag=False):
        if include_flag:
            self.step_args.append("--release-id")
        self.step_args.append(release_id)
        return self

    def with_custom_arg(self, arg: str):
        self.step_args.append(arg)
        return self

    def with_custom_args(self, args: list):
        self.step_args.extend(args)
        return self

    def build(self):
        return self.step_args


class EmrStepBuilder:
    def __init__(self, job_name: str, step_args: list):
        self.job_name = job_name
        self.step_args = step_args
        self.action_on_failure = "CONTINUE"

    def build(self):
        return {
            "HadoopJarStep": {
                "Args": self.step_args,
                "Jar": "command-runner.jar"
            },
            "Name": f"{self.job_name}",
            "ActionOnFailure": f"{self.action_on_failure}"
        }


variant_etl_map = {

    'download and run fhavro-export': lambda etl_config, elastic_search_endpoint:
    EmrStepBuilder(
        "Normalize Dataservice",
        step_args=EmrStepArgumentBuilder()
        .with_spark_job(
            "bio.ferlab.etl.normalized.dataservice.RunNormalizeDataservice", "bucket")
        .with_packages(["com.typesafe.play:play-ahc-ws-standalone_2.12:2.0.3"])
        .with_config("qa", "kf-strides", include_flag=True)
        .with_steps()
        .with_release_id("re_00q")
        .with_studies(studies=["g", "t"], include_flag=True)
        .build()
    )
    .build()
}
if __name__ == '__main__':
    env = "qa"
    account = "kf-strides"
    release_id = "re_0001"
    study_ids = ["test1", "test2"]
    print(variant_etl_map["download and run fhavro-export"]("", ""))

    list1 = [1,2]
    print(not list1)
