import json


class dbtManifest:
    def __init__(self, manifest_path):
        self.manifest_path = manifest_path
        self.manifest = self.parse_manifest()
        self.run_result = self.parse_run_result()

    @classmethod
    def _parse_manifest(self):
        models = []
        with open(self.manifest_path, "r") as file:
            data = json.load(file)
        for model in data.get("nodes", {}).values():
            if model["resource_type"] == "model":
                models.append(
                    {
                        "name": model["name"],
                        "sql": model["compiled_sql"],
                        "unique_id": model["unique_id"],
                    }
                )
        return models


class dbtRunResults:
    @classmethod
    def parse_run_result(self, run_result_path):
        pass


class dbtArtefacts:

    def __init__(
        self,
        manifest_path="./target/manifest.json",
        run_result_path="./target/run_result.json",
    ):
        self.manifest = dbtManifest(manifest_path)
        self.run_result = dbtRunResults(run_result_path)
