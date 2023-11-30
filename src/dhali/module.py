import json
import requests

from requests.models import Response
from typing import Dict, Union, IO, Any, BinaryIO
from xrpl import wallet


class Module:
    """
    An interface to modules that are available in the Dhali marketplace.
    """

    def __init__(self, asset_uuid: str):
        self.asset_uuid = asset_uuid
        self.url = f"https://dhali-prod-run-dauenf0n.uc.gateway.dev/{asset_uuid}/run/"

    def run(
        self, input_file: Union[IO[bytes], BinaryIO], payment_claim: Dict[str, Any]
    ) -> Response:
        """
        Sends 'input' to this Dhali module, if 'payment_claim' is valid.  If 'payment_claim' is not valid,
        the returned `Response` will have a 402 error code.

        Args:
            input (Union[IO[bytes], BinaryIO]): the input to be sent to Dhali
            payment_claim (Dict[str, Any]): a valid payment claim, against an open payment channel

        Returns:
            Response: the response returned from Dhali

        Raises:
            ValueError: If 'input_file' is not opened in binary mode.
            AttributeError: If 'input_file' does not have a 'read' method.
        """
        if hasattr(input_file, "mode") and input_file.mode != "rb":
            raise ValueError("input_file must be opened in binary mode (rb)")

        if not hasattr(input_file, "read"):
            raise AttributeError("Input file must have a 'read' method.")

        headers = {"Payment-Claim": json.dumps(payment_claim)}
        files = {"input": input_file}

        return requests.put(self.url, headers=headers, files=files)
