"""Scaffold tests for CDK cleaner."""

import io

from pretty_cfn.cdk_cleaner import CDKCleaner, is_cdk_hash, strip_hash_suffix
from pretty_cfn.formatter import CFNTag, create_cfn_yaml


def load_yaml(s: str):
    yaml = create_cfn_yaml()
    return yaml.load(s)


def test_is_cdk_hash_and_strip():
    assert is_cdk_hash("MyBucketF68F3FF0")
    assert not is_cdk_hash("MyBucket")
    assert strip_hash_suffix("MyBucketF68F3FF0") == "MyBucket"
    assert strip_hash_suffix("Name123") == "Name123"


def test_simple_rename_and_ref_update():
    doc = load_yaml(
        """
Resources:
  MyBucketF68F3FF0:
    Type: AWS::S3::Bucket
Outputs:
  BucketArn:
    Value: !GetAtt MyBucketF68F3FF0.Arn
"""
    )

    cleaner = CDKCleaner(mode="readable")
    out = cleaner.clean(doc)

    # Resource renamed
    assert "MyBucket" in out["Resources"]
    assert "MyBucketF68F3FF0" not in out["Resources"]

    # GetAtt updated
    val = out["Outputs"]["BucketArn"]["Value"]
    assert isinstance(val, CFNTag) and val.tag == "GetAtt"
    assert val.value == "MyBucket.Arn"


def test_getatt_list_and_sub_updates():
    doc = load_yaml(
        """
Resources:
  MyRole3C357FF2:
    Type: AWS::IAM::Role
  MyFnABCDEF12:
    Type: AWS::Lambda::Function
    Properties:
      Role: !GetAtt [ MyRole3C357FF2, Arn ]
Outputs:
  RoleRef:
    Value: !Sub "arn:aws:iam::${AWS::AccountId}:role/${MyRole3C357FF2}"
"""
    )

    out = CDKCleaner(mode="readable").clean(doc)

    # Renamed resources
    assert "MyRole" in out["Resources"]
    # Updated GetAtt list form
    role = out["Resources"]["MyFn"]["Properties"]["Role"]
    assert isinstance(role, CFNTag) and role.tag == "GetAtt"
    assert role.value[0] == "MyRole"
    # Updated Sub token
    sub = out["Outputs"]["RoleRef"]["Value"]
    assert isinstance(sub, CFNTag) and sub.tag == "Sub"
    assert "${MyRole}" in sub.value


def test_remove_cdk_metadata():
    doc = load_yaml(
        """
Resources:
  CDKMetadata:
    Type: AWS::CDK::Metadata
  BucketX123ABCD:
    Type: AWS::S3::Bucket
"""
    )

    out = CDKCleaner(mode="readable").clean(doc)
    assert "CDKMetadata" not in out["Resources"]
    assert any(True for k in out["Resources"].keys() if k.startswith("Bucket"))


def test_keep_path_metadata_flag_behavior():
    doc = load_yaml(
        """
Resources:
  R1:
    Type: AWS::S3::Bucket
    Metadata:
      aws:cdk:path: App/R1/Resource
      aws:asset:path: foo
"""
    )
    out1 = CDKCleaner(mode="readable", keep_path_metadata=True).clean(doc)
    # Find the (possibly renamed) resource
    rname = next(iter(out1["Resources"].keys()))
    md1 = out1["Resources"][rname]["Metadata"]
    assert "aws:cdk:path" in md1 and "aws:asset:path" not in md1

    out2 = CDKCleaner(mode="readable", keep_path_metadata=False).clean(doc)
    md2 = out2["Resources"]["R1"].get("Metadata", {})
    assert "aws:cdk:path" not in md2


def test_semantic_naming_patterns_applied():
    doc = load_yaml(
        """
Resources:
  MyFunctionServiceRole3C357FF2:
    Type: AWS::IAM::Role
    Metadata: { aws:cdk:path: App/MyFunction/ServiceRole/Resource }
  MyFunctionServiceRoleDefaultPolicy4E8A5F5D:
    Type: AWS::IAM::Policy
    Metadata: { aws:cdk:path: App/MyFunction/ServiceRole/DefaultPolicy/Resource }
  MyFunctionLogGroupABCDEF12:
    Type: AWS::Logs::LogGroup
    Metadata: { aws:cdk:path: App/MyFunction/LogGroup/Resource }
"""
    )
    out = CDKCleaner(mode="readable", semantic_naming=True).clean(doc)
    names = set(out["Resources"].keys())
    assert "MyFunctionRole" in names
    assert "MyFunctionPolicy" in names
    assert "MyFunctionLogs" in names


def test_collision_strategy_short_hash():
    doc = load_yaml(
        """
Resources:
  ItemA12345678:
    Type: AWS::S3::Bucket
    Metadata: { aws:cdk:path: App/Item/Resource }
  ItemB87654321:
    Type: AWS::S3::Bucket
    Metadata: { aws:cdk:path: App/Item/Resource }
"""
    )
    out = CDKCleaner(mode="readable", collision_strategy="short-hash").clean(doc)
    keys = list(out["Resources"].keys())
    assert keys[0].startswith("Item") and keys[1].startswith("Item") and keys[0] != keys[1]


def test_asset_parameters_cleanup_readable_mode():
    doc = load_yaml(
        """
Parameters:
  AssetParametersABCDS3Bucket: { Type: String }
  AssetParametersABCDS3VersionKey: { Type: String }
Resources:
  Fn:
    Type: AWS::Lambda::Function
    Properties:
      Code:
        S3Bucket: !Ref AssetParametersABCDS3Bucket
        S3Key: !Ref AssetParametersABCDS3VersionKey
"""
    )
    out = CDKCleaner(mode="readable").clean(doc)
    assert out.get("Parameters") is None or "AssetParametersABCDS3Bucket" not in out.get(
        "Parameters", {}
    )
    code = out["Resources"]["Fn"]["Properties"]["Code"]
    assert code["S3Bucket"] == "<asset-bucket>"
    assert code["S3Key"] == "<asset-key>"


def test_remove_cdkmetadata_condition():
    doc = load_yaml(
        """
Resources:
  CDKMetadata:
    Type: AWS::CDK::Metadata
Conditions:
  CDKMetadataAvailable: { Fn::Equals: [ true, true ] }
"""
    )
    out = CDKCleaner(mode="readable").clean(doc)
    assert "Conditions" not in out or "CDKMetadataAvailable" not in out.get("Conditions", {})


def test_json_style_intrinsics_are_updated():
    doc = {
        "Resources": {
            "MyFnABCDEF12": {
                "Type": "AWS::Lambda::Function",
                "Properties": {"Role": {"Fn::GetAtt": ["MyRole3C357FF2", "Arn"]}},
            },
            "MyRole3C357FF2": {"Type": "AWS::IAM::Role"},
        },
        "Outputs": {
            "RoleRef": {"Value": {"Ref": "MyRole3C357FF2"}},
            "RoleArn": {"Value": {"Fn::GetAtt": "MyRole3C357FF2.Arn"}},
            "Sub": {"Value": {"Fn::Sub": "${MyRole3C357FF2}"}},
        },
    }

    out = CDKCleaner(mode="readable").clean(doc)
    assert "MyRole" in out["Resources"]
    # Ref updated
    assert out["Outputs"]["RoleRef"]["Value"]["Ref"] == "MyRole"
    # GetAtt (list) updated
    assert out["Resources"]["MyFn"]["Properties"]["Role"]["Fn::GetAtt"][0] == "MyRole"
    # GetAtt (string) updated
    assert out["Outputs"]["RoleArn"]["Value"]["Fn::GetAtt"] == "MyRole.Arn"
    # Sub updated
    assert out["Outputs"]["Sub"]["Value"]["Fn::Sub"] == "${MyRole}"


def test_if_short_form_is_updated():
    doc = load_yaml(
        """
Resources:
  MyBucketF68F3FF0:
    Type: AWS::S3::Bucket
Outputs:
  MaybeArn:
    Value: !If [IsProd, !GetAtt MyBucketF68F3FF0.Arn, !Ref MyBucketF68F3FF0]
"""
    )

    out = CDKCleaner(mode="readable").clean(doc)
    value = out["Outputs"]["MaybeArn"]["Value"]
    assert isinstance(value, CFNTag) and value.tag == "If"
    assert any(
        (isinstance(item, CFNTag) and item.tag == "GetAtt" and item.value == "MyBucket.Arn")
        for item in value.value
    )


def test_cdk_clean_preserves_resource_header_comment():
    doc = load_yaml(
        """
Resources:

  # Header comment for Bucket
  MyBucketF68F3FF0:
    Type: AWS::S3::Bucket
"""
    )

    out = CDKCleaner(mode="readable").clean(doc)
    yaml = create_cfn_yaml()
    buf = io.StringIO()
    yaml.dump(out, buf)
    rendered = buf.getvalue()

    assert "# Header comment for Bucket" in rendered
    assert "MyBucketF68F3FF0" not in rendered
    assert "MyBucket" in rendered
