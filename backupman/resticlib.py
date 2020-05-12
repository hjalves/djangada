import logging
import json
import subprocess

logger = logging.getLogger(__name__)


class ResticWrapper:
    def __init__(self, repository, password, env, cache_dir=None):
        self.bin = "/usr/bin/restic"
        self.env = {
            "RESTIC_CACHE_DIR": cache_dir or "/home/halves/.cache/restic/",
            "RESTIC_REPOSITORY": repository,
            "RESTIC_PASSWORD": password,
            **env,
        }

    def call(self, *a, **kw):
        kw = [f"--{k}" if v is True else f"--{k}={v}" for k, v in kw.items()]
        args = [self.bin, *a, *kw]
        logger.debug("running: %s", " ".join(args))
        result = subprocess.run(args, capture_output=True, text=True, env=self.env)
        logger.debug("exit code: %s", result.returncode)
        if result.stderr:
            logger.debug("stderr: %s", result.stderr)
        result.check_returncode()
        return result.stdout

    def snapshots(self):
        output = self.call("snapshots", json=True)
        return json.loads(output)

    def ls(self, snapshot, *args):
        output = self.call("ls", snapshot, *args, json=True)
        result = []
        for d in (json.loads(x) for x in output.split("\n") if x.strip()):
            if d.pop("struct_type") == "node":
                result.append(d)
        return result

    def stats(self, *args):
        output = self.call("stats", *args, json=True)
        return json.loads(output)
