"""CDK metadata loader and parser for accurate construct mappings."""

import json
import re
from pathlib import Path
from typing import Any, Dict, Optional, Union


class CDKMetadataLoader:
    """Loads and parses CDK metadata files for accurate construct mappings."""

    @staticmethod
    def load(path: Union[str, Path]) -> Dict[str, Any]:
        """
        Load CDK metadata from cdk.out directory or manifest file.

        Args:
            path: Path to cdk.out directory or manifest.json file

        Returns:
            Dictionary mapping logical IDs to construct information:
            {
                "LogicalID": {
                    "path": "/StackName/ConstructName/Resource",
                    "construct_name": "ConstructName",
                    "is_generated": False,
                    "resource_type": "AWS::ECS::Cluster"  # if available from tree.json
                }
            }
        """
        path = Path(path)

        if path.is_dir():
            # Load from cdk.out directory
            return CDKMetadataLoader._load_from_directory(path)
        elif path.is_file():
            # Load single manifest or tree file
            payload = json.loads(path.read_text())
            if "artifacts" in payload:
                return CDKMetadataLoader._extract_mappings(payload)
            if "tree" in payload or payload.get("version", "").startswith("tree"):
                return CDKMetadataLoader._extract_tree_mappings(payload)
            raise ValueError(f"Unrecognized CDK metadata file: {path}")
        else:
            raise ValueError(f"Path does not exist: {path}")

    @staticmethod
    def _load_from_directory(cdk_out: Path) -> Dict[str, Any]:
        """Load metadata from cdk.out directory."""
        mappings = {}

        # Try to load manifest.json
        manifest_path = cdk_out / "manifest.json"
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text())
            mappings = CDKMetadataLoader._extract_mappings(manifest)

        # Try to enrich with tree.json
        tree_path = cdk_out / "tree.json"
        if tree_path.exists():
            tree = json.loads(tree_path.read_text())
            mappings = CDKMetadataLoader._enrich_with_tree(mappings, tree)

        return mappings

    @staticmethod
    def _extract_mappings(manifest: dict) -> Dict[str, Any]:
        """Extract logical ID to construct name mappings from manifest."""
        mappings = {}

        # Process each stack artifact
        for artifact_name, artifact in manifest.get("artifacts", {}).items():
            if artifact.get("type") != "aws:cloudformation:stack":
                continue

            # Process metadata entries
            for path, metadata_list in artifact.get("metadata", {}).items():
                for item in metadata_list:
                    if item.get("type") == "aws:cdk:logicalId":
                        logical_id = item.get("data")
                        if logical_id:
                            construct_name = CDKMetadataLoader._extract_construct_name(path)
                            mappings[logical_id] = {
                                "path": path,
                                "construct_name": construct_name,
                                "is_generated": CDKMetadataLoader._is_generated_resource(
                                    path, construct_name
                                ),
                                "stack_name": artifact_name,
                            }

        return mappings

    @staticmethod
    def _extract_tree_mappings(tree_doc: dict) -> Dict[str, Any]:
        """Extract mappings when only tree.json-like data is available."""

        resource_info = CDKMetadataLoader._extract_resource_info(tree_doc.get("tree", tree_doc))
        mappings: Dict[str, Any] = {}
        for path, info in resource_info.items():
            logical_id = info.get("logical_id")
            if not logical_id:
                continue
            construct_name = CDKMetadataLoader._extract_construct_name(path)
            mappings[logical_id] = {
                "path": path,
                "construct_name": construct_name,
                "is_generated": CDKMetadataLoader._is_generated_resource(path, construct_name),
            }
            if "resource_type" in info:
                mappings[logical_id]["resource_type"] = info["resource_type"]
        return mappings

    @staticmethod
    def _extract_construct_name(path: str) -> str:
        """
        Extract the construct name from a CDK path.

        Examples:
            /Stack/Vpc/Resource -> Vpc
            /Stack/Vpc/PublicSubnet1/Subnet -> VpcPublicSubnet1Subnet
            /Stack/Vpc/PublicSubnet1/RouteTable -> VpcPublicSubnet1RouteTable
            /Stack/Service -> Service
        """
        # Remove leading slash and split
        parts = path.strip("/").split("/")

        if not parts:
            return ""

        # Remove stack name (first part)
        if len(parts) > 1:
            parts = parts[1:]
        else:
            return parts[0]

        # Remove trailing "Resource" if present
        if parts[-1] == "Resource":
            parts = parts[:-1]

        # For nested resources, we need to preserve uniqueness
        if len(parts) > 1:
            # For VPC-related resources, combine the parent and leaf names
            # e.g., Vpc/PublicSubnet1/Subnet -> VpcPublicSubnet1Subnet
            # e.g., Vpc/PublicSubnet1/RouteTable -> VpcPublicSubnet1RouteTable
            if len(parts) == 2:
                # Two-level nesting: combine both
                return "".join(parts)
            elif len(parts) >= 3:
                # Three or more levels: use middle and last
                # e.g., Vpc/PublicSubnet1/RouteTableAssociation -> VpcPublicSubnet1RouteTableAssociation
                if parts[0] == "Vpc" and len(parts) == 3:
                    return "".join(parts[1:])  # Skip "Vpc" prefix to avoid redundancy
                else:
                    # For deep nesting, take the last two meaningful parts
                    return "".join(parts[-2:])
            else:
                return parts[-1]

        return parts[0] if parts else ""

    @staticmethod
    def _is_generated_resource(path: str, construct_name: str) -> bool:
        """
        Determine if a resource is CDK-generated or user-defined.

        CDK-generated resources typically include:
        - Resources with paths ending in /Resource
        - Resources with nested paths like /Construct/SubConstruct/Resource
        - Resources with specific patterns like ServiceRole, DefaultPolicy
        """
        # Resources explicitly marked as "Resource" are usually CDK-generated wrappers
        if path.endswith("/Resource"):
            return True

        # Common CDK-generated patterns
        generated_patterns = [
            r"ServiceRole[A-F0-9]{8}$",
            r"DefaultPolicy[A-F0-9]{8}$",
            r"LogGroup[A-F0-9]{8}$",
            r"SecurityGroup[A-F0-9]{8}$",
        ]

        for pattern in generated_patterns:
            if re.search(pattern, construct_name):
                return True

        # Deep nested paths often indicate generated resources
        path_parts = path.strip("/").split("/")
        if len(path_parts) > 3:
            return True

        return False

    @staticmethod
    def _enrich_with_tree(mappings: Dict[str, Any], tree: dict) -> Dict[str, Any]:
        """
        Enrich mappings with information from tree.json.

        Tree.json contains the construct tree with CloudFormation resource types.
        """
        # The tree structure has a root "tree" key
        tree_root = tree.get("tree", tree)

        # Build a path to resource type and logical ID mapping from tree
        resource_info = CDKMetadataLoader._extract_resource_info(tree_root)

        # Enrich each mapping with resource type if available
        for logical_id, info in mappings.items():
            # Try to find this logical ID in the tree
            for tree_path, tree_data in resource_info.items():
                if tree_data.get("logical_id") == logical_id:
                    if "resource_type" in tree_data:
                        info["resource_type"] = tree_data["resource_type"]
                    break

        return mappings

    @staticmethod
    def _extract_resource_info(node: dict, current_path: str = "") -> Dict[str, dict]:
        """
        Recursively extract resource information from tree.json.

        Returns mapping of CDK path to resource info including type and logical ID.
        """
        resource_info = {}

        # Check if this node has CloudFormation metadata
        attributes = node.get("attributes", {})
        cfn_type = attributes.get("aws:cdk:cloudformation:type")
        # cfn_props = attributes.get("aws:cdk:cloudformation:props", {})  # Not currently used

        # The logical ID might be in the node id or in metadata
        node_id = node.get("id", "")

        if current_path:
            info = {}
            if cfn_type:
                info["resource_type"] = cfn_type
            # Try to extract logical ID from the node ID (often matches)
            if node_id and not node_id.startswith("$"):  # Skip special nodes
                info["logical_id"] = node_id
            if info:
                resource_info[current_path] = info

        # Process children
        children = node.get("children", {})
        for child_id, child_node in children.items():
            child_path = f"{current_path}/{child_id}" if current_path else f"/{child_id}"
            child_info = CDKMetadataLoader._extract_resource_info(child_node, child_path)
            resource_info.update(child_info)

        return resource_info

    @staticmethod
    def find_template_file(cdk_out: Path) -> Optional[Path]:
        """
        Find the CloudFormation template file in cdk.out directory.

        Returns the first .template.json file found.
        """
        for template_file in cdk_out.glob("*.template.json"):
            return template_file
        return None
