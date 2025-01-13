from typing import Any

from moto.core.exceptions import RESTError

ERROR_WITH_ACCESS_POINT_NAME = """{% extends 'wrapped_single_error' %}
{% block extra %}<AccessPointName>{{ name }}</AccessPointName>{% endblock %}
"""


ERROR_WITH_ACCESS_POINT_POLICY = """{% extends 'wrapped_single_error' %}
{% block extra %}<AccessPointName>{{ name }}</AccessPointName>{% endblock %}
"""

ERROR_WITH_OPERATION_NAME = """{% extends 'wrapped_single_error' %}
{% block extra %}<CreateJob>{{ name }}</CreateJob>{% endblock %}
"""


class S3ControlError(RESTError):
    extended_templates = {
        "ap_not_found": ERROR_WITH_ACCESS_POINT_NAME,
        "apf_not_found": ERROR_WITH_ACCESS_POINT_POLICY,
        "operation_not_found": ERROR_WITH_OPERATION_NAME,
    }
    env = RESTError.extended_environment(extended_templates)

    def __init__(self, *args: Any, **kwargs: Any):
        kwargs.setdefault("template", "single_error")
        super().__init__(*args, **kwargs)


class AccessPointNotFound(S3ControlError):
    code = 404

    def __init__(self, name: str, **kwargs: Any):
        kwargs.setdefault("template", "ap_not_found")
        kwargs["name"] = name
        super().__init__(
            "NoSuchAccessPoint", "The specified accesspoint does not exist", **kwargs
        )


class AccessPointPolicyNotFound(S3ControlError):
    code = 404

    def __init__(self, name: str, **kwargs: Any):
        kwargs.setdefault("template", "apf_not_found")
        kwargs["name"] = name
        super().__init__(
            "NoSuchAccessPointPolicy",
            "The specified accesspoint policy does not exist",
            **kwargs,
        )


class ValidationError(S3ControlError):
    pass


class InvalidJobOperation(S3ControlError):
    def __init__(self, name: str, **kwargs: Any):
        kwargs.setdefault("template", "operation_not_found")
        kwargs["name"] = name
        super().__init__(
            "BadRequestException", "The specified operation does not exist", **kwargs
        )


class JobNotFoundException(RESTError):
    code = 400

    def __init__(self):
        super().__init__(
            "InvalidRequest", "Job not found", template="wrapped_single_error"
        )
