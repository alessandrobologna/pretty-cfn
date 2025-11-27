"""Tests for CDK metadata loading and parsing."""

import json
import tempfile
from pathlib import Path

from pretty_cfn.cdk_metadata import CDKMetadataLoader


class TestCDKMetadataLoader:
    """Tests for CDKMetadataLoader."""

    def test_extract_construct_name_simple(self):
        """Test simple construct name extraction."""
        assert CDKMetadataLoader._extract_construct_name("/Stack/Vpc/Resource") == "Vpc"
        assert CDKMetadataLoader._extract_construct_name("/Stack/Service") == "Service"
        assert CDKMetadataLoader._extract_construct_name("/Stack/Cluster/Resource") == "Cluster"

    def test_extract_construct_name_nested(self):
        """Test nested construct name extraction."""
        # VPC subnets
        assert (
            CDKMetadataLoader._extract_construct_name("/Stack/Vpc/PublicSubnet1/Subnet")
            == "PublicSubnet1Subnet"
        )
        assert (
            CDKMetadataLoader._extract_construct_name("/Stack/Vpc/PublicSubnet1/RouteTable")
            == "PublicSubnet1RouteTable"
        )
        assert (
            CDKMetadataLoader._extract_construct_name(
                "/Stack/Vpc/PublicSubnet1/RouteTableAssociation"
            )
            == "PublicSubnet1RouteTableAssociation"
        )

    def test_is_generated_resource(self):
        """Test detection of CDK-generated resources."""
        # Resources ending in /Resource are generated
        assert CDKMetadataLoader._is_generated_resource("/Stack/Vpc/Resource", "Vpc")

        # Deep nested paths are usually generated
        assert CDKMetadataLoader._is_generated_resource(
            "/Stack/Vpc/PublicSubnet1/RouteTable", "PublicSubnet1RouteTable"
        )

        # Simple paths are usually user-defined
        assert not CDKMetadataLoader._is_generated_resource("/Stack/Service", "Service")

    def test_load_manifest_json(self):
        """Test loading manifest.json file."""
        manifest = {
            "version": "1.0.0",
            "artifacts": {
                "TestStack": {
                    "type": "aws:cloudformation:stack",
                    "metadata": {
                        "/TestStack/Vpc/Resource": [
                            {"type": "aws:cdk:logicalId", "data": "Vpc8378EB38"}
                        ],
                        "/TestStack/Service": [
                            {"type": "aws:cdk:logicalId", "data": "ServiceABC123"}
                        ],
                    },
                }
            },
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(manifest, f)
            manifest_path = Path(f.name)

        try:
            mappings = CDKMetadataLoader.load(manifest_path)

            assert len(mappings) == 2
            assert "Vpc8378EB38" in mappings
            assert mappings["Vpc8378EB38"]["construct_name"] == "Vpc"
            assert mappings["Vpc8378EB38"]["path"] == "/TestStack/Vpc/Resource"
            assert mappings["Vpc8378EB38"]["is_generated"] is True

            assert "ServiceABC123" in mappings
            assert mappings["ServiceABC123"]["construct_name"] == "Service"
            assert mappings["ServiceABC123"]["path"] == "/TestStack/Service"
            assert mappings["ServiceABC123"]["is_generated"] is False
        finally:
            manifest_path.unlink()

    def test_load_cdk_out_directory(self):
        """Test loading from cdk.out directory structure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cdk_out = Path(tmpdir)

            # Create manifest.json
            manifest = {
                "version": "1.0.0",
                "artifacts": {
                    "TestStack": {
                        "type": "aws:cloudformation:stack",
                        "metadata": {
                            "/TestStack/Bucket/Resource": [
                                {"type": "aws:cdk:logicalId", "data": "BucketABC"}
                            ]
                        },
                    }
                },
            }
            (cdk_out / "manifest.json").write_text(json.dumps(manifest))

            # Create tree.json (simplified)
            tree = {
                "version": "tree-0.1",
                "tree": {
                    "id": "App",
                    "children": {
                        "TestStack": {
                            "id": "TestStack",
                            "children": {
                                "Bucket": {
                                    "id": "Bucket",
                                    "children": {
                                        "Resource": {
                                            "id": "Resource",
                                            "attributes": {
                                                "aws:cdk:cloudformation:type": "AWS::S3::Bucket"
                                            },
                                        }
                                    },
                                }
                            },
                        }
                    },
                },
            }
            (cdk_out / "tree.json").write_text(json.dumps(tree))

            mappings = CDKMetadataLoader.load(cdk_out)

            assert len(mappings) == 1
            assert "BucketABC" in mappings
            assert mappings["BucketABC"]["construct_name"] == "Bucket"

    def test_find_template_file(self):
        """Test finding template file in cdk.out."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cdk_out = Path(tmpdir)

            # Create a template file
            template_file = cdk_out / "MyStack.template.json"
            template_file.write_text("{}")

            found = CDKMetadataLoader.find_template_file(cdk_out)
            assert found == template_file

            # Test no template file
            template_file.unlink()
            found = CDKMetadataLoader.find_template_file(cdk_out)
            assert found is None

    def test_load_tree_only_file(self, tmp_path: Path):
        """Tree.json without manifest should still produce mappings."""

        tree = {
            "version": "tree-0.1",
            "tree": {
                "id": "App",
                "children": {
                    "MyStack": {
                        "id": "MyStack",
                        "children": {
                            "Bucket": {
                                "id": "Bucket",
                                "children": {
                                    "Resource": {
                                        "id": "Resource",
                                        "attributes": {
                                            "aws:cdk:cloudformation:type": "AWS::S3::Bucket"
                                        },
                                    }
                                },
                            }
                        },
                    }
                },
            },
        }

        tree_path = tmp_path / "tree.json"
        tree_path.write_text(json.dumps(tree))

        mappings = CDKMetadataLoader.load(tree_path)
        assert mappings
        # The logical ID defaults to the node id when using tree metadata
        assert any(info.get("construct_name") for info in mappings.values())
