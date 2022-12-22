# -*- coding: utf-8 -*-
# *******************************************************
#   ____                     _               _
#  / ___|___  _ __ ___   ___| |_   _ __ ___ | |
# | |   / _ \| '_ ` _ \ / _ \ __| | '_ ` _ \| |
# | |__| (_) | | | | | |  __/ |_ _| | | | | | |
#  \____\___/|_| |_| |_|\___|\__(_)_| |_| |_|_|
#
#  Sign up for free at http://www.comet.ml
#  Copyright (C) 2015-2021 Comet ML INC
#  This file can not be copied and/or distributed without
#  the express permission of Comet ML Inc.
# *******************************************************
import logging

import requests

LOGGER = logging.getLogger(__name__)
DEFAULT_JUPYTER_INTERACTIVE_FILE_NAME = "Jupyter interactive"
DEFAULT_JUPYTER_CODE_ASSET_NAME = "Code.ipynb"
DEFAULT_COLAB_NOTEBOOK_ASSET_NAME = "ColabNotebook.ipynb"
COLAB_SESSION_URL = "http://172.28.0.2:9000/api/sessions"


def _in_jupyter_environment():
    # type: () -> bool
    """
    Check to see if code is running in a Jupyter environment,
    including jupyter notebook, lab, or console.
    """
    try:
        import IPython
    except Exception:
        return False

    ipy = IPython.get_ipython()
    if ipy is None or not hasattr(ipy, "kernel"):
        return False
    else:
        return True


def _in_ipython_environment():
    # type: () -> bool
    """
    Check to see if code is running in an IPython environment.
    """
    try:
        import IPython
    except Exception:
        return False

    ipy = IPython.get_ipython()
    if ipy is None:
        return False
    else:
        return True


def _in_colab_environment():
    # type: () -> bool
    """
    Check to see if code is running in Google colab.
    """
    try:
        import IPython
    except Exception:
        return False

    ipy = IPython.get_ipython()
    return "google.colab" in str(ipy)


def _get_notebook_url():
    try:
        notebook_file_id = requests.get(COLAB_SESSION_URL, timeout=1).json()[0]["path"]
        return "https://colab.research.google.com/notebook#%s" % notebook_file_id
    except Exception:
        LOGGER.debug("Failed to retrieve the fileId of the notebook", exc_info=True)
        return None


def _get_colab_notebook_json():
    try:
        import google.colab._message

        notebook_json = google.colab._message.blocking_request(request_type="get_ipynb")
    except Exception:
        LOGGER.debug("Failed to get Google Colab notebook content", exc_info=True)
        return None

    try:
        return notebook_json["ipynb"]
    except Exception:
        LOGGER.debug("Failed to parse Google Colab payload", exc_info=True)
        return None


def display_or_open_browser(url, clear=False, wait=True, new=0, autoraise=True):
    # type: (str, bool, bool, int, bool) -> None
    if _in_jupyter_environment():
        from IPython.display import IFrame, clear_output, display

        if clear:
            clear_output(wait=wait)
        display(IFrame(src=url, width="100%", height="800px"))
    else:
        import webbrowser

        webbrowser.open(url, new=new, autoraise=autoraise)
