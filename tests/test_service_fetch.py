from collections import OrderedDict
import json
import sys
import types

import yaml

from pretty_cfn.service import fetch_stack_template


class _FakeClient:
    def __init__(self, template_body):
        self._template_body = template_body

    def get_template(self, **_kwargs):
        return {"TemplateBody": self._template_body}


def _install_boto3_stub(monkeypatch, template_body):
    class _Session:
        def client(self, name):
            assert name == "cloudformation"
            return _FakeClient(template_body)

    fake_boto3 = types.SimpleNamespace(session=types.SimpleNamespace(Session=_Session))
    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)


def test_fetch_stack_template_serializes_ordered_dict(monkeypatch):
    template = OrderedDict(
        {
            "Resources": OrderedDict(
                {
                    "Bucket": OrderedDict(
                        {
                            "Type": "AWS::S3::Bucket",
                            "Properties": OrderedDict({"BucketName": "demo"}),
                        }
                    )
                }
            )
        }
    )

    _install_boto3_stub(monkeypatch, template)

    rendered = fetch_stack_template("DemoStack")

    assert isinstance(rendered, str)
    parsed = yaml.safe_load(rendered)
    expected = json.loads(json.dumps(template))
    assert parsed == expected


def test_fetch_stack_template_passes_through_string(monkeypatch):
    json_body = '{"Resources":{}}'
    _install_boto3_stub(monkeypatch, json_body)

    rendered = fetch_stack_template("DemoStack")

    assert rendered == json_body
