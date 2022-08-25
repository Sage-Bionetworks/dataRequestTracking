'''
Name: test_scheduledJob.py
'''
# import modules
import json
import logging
import os
from datetime import datetime
from functools import partial
from itertools import chain

import numpy as np
import pandas as pd
import pytz
import synapseclient
from synapseclient import File, Table
from synapseclient.core.exceptions import (SynapseAuthenticationError,
                                           SynapseNoCredentialsError)
from synapseutils import walk

# adapted from challengeutils https://github.com/Sage-Bionetworks/challengeutils/pull/121/files to manage Synapse connection
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

class Synapse:
    """Define Synapse class"""

    _synapse_client = None

    @classmethod
    def client(cls, syn_user=None, syn_pass=None, *args, **kwargs):
        """Gets a logged in instance of the synapseclient.
        Args:
            syn_user: Synapse username
            syn_pass: Synpase password
        Returns:
            logged in synapse client
        """
        if not cls._synapse_client:
            LOGGER.debug("Getting a new Synapse client.")
            cls._synapse_client = synapseclient.Synapse(*args, **kwargs)
            try:
                if os.getenv("SCHEDULED_JOB_SECRETS") is not None:
                    secrets = json.loads(os.getenv("SCHEDULED_JOB_SECRETS"))
                    cls._synapse_client.login(silent=True, authToken=secrets["SYNAPSE_AUTH_TOKEN"])
                else:
                    cls._synapse_client.login(silent=True)
            except SynapseAuthenticationError:
                cls._synapse_client.login(syn_user, syn_pass, silent=True)

        LOGGER.debug("Already have a Synapse client, returning it.")
        return cls._synapse_client

    @classmethod
    def reset(cls):
        """Change synapse connection"""
        cls._synapse_client = None

def prGreen(t, st=None):  # print for success
    """Function to print out message in green

    Args:
        t (str): a string
        st (str, optional): the latter string. Defaults to None. 
        Use this when you want to print out the latter string.
    """
    if st is None:
        print("\033[92m {}\033[00m".format(t))
    else:
        print("{}: \033[92m {}\033[00m".format(t, st))

def main():
    #syn = synapseclient.Synapse()
    #syn.login(silent=True)
    Synapse().client
    print('Horray!')
if __name__ == "__main__":
    main()
