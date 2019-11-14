import os
from functools import partial
import s3fs
from allensdk.api.caching_utilities import one_file_call_caching
from allensdk.brain_observatory.ecephys.ecephys_project_cache import EcephysProjectCache, read_nwb
from allensdk.brain_observatory.ecephys.ecephys_session import EcephysSession
from allensdk.config.manifest import Manifest


class S3Cache(EcephysProjectCache):
    
    def __init__(self, *args, **kwargs):
        super(S3Cache, self).__init__(*args, **kwargs)
        self.s3fs = s3fs.S3FileSystem(anon=True)
    
    def _get_s3_path(self, path):
        return path.replace(
            os.path.abspath(
                os.path.dirname(
                    self.manifest_path)
            ), 
            "s3://allen-brain-observatory/visual-coding-neuropixels/ecephys-cache"
        )
    
    def get_session_data(self, session_id: int, filter_by_validity: bool = True, **unit_filter_kwargs):
        """ Obtain an EcephysSession object containing detailed data for a single session
        """

        path = self.get_cache_path(None, self.SESSION_NWB_KEY, session_id, session_id)

        def read(_path):
            session_api = self._build_nwb_api_for_session(_path, session_id, filter_by_validity, **unit_filter_kwargs)
            return EcephysSession(api=session_api, test=True)
        
        Manifest.safe_make_parent_dirs(path)
        return one_file_call_caching(
            path,
            partial(self.s3fs.get, self._get_s3_path(path), path),
            lambda *a, **k: None,
            read,
            num_tries=self.fetch_tries
        )
    

    def _setup_probe_promises(self, session_id):
        probes = self.get_probes()
        probe_ids = probes[probes["ecephys_session_id"] == session_id].index.values
        
        out = {}
        for probe_id in probe_ids:
            path = self.get_cache_path(None, self.PROBE_LFP_NWB_KEY, session_id, probe_id)
            out[probe_id] = partial( 
                one_file_call_caching,
                path,
                partial(self.s3fs.get, self._get_s3_path(path), path),
                lambda *a, **k: None,
                read_nwb,
                num_tries=self.fetch_tries
            )
        return out