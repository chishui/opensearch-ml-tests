"""
Template management for OpenSearch queries and documents.
"""
import json
from typing import Dict, Any, Optional


class Template:
    """Base template class with rendering capabilities."""
    
    def __init__(self, template_str: str):
        self.template_str = template_str.strip()
    
    def render(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Render the template with the given parameters."""
        result = self.template_str
        for key, value in params.items():
            placeholder = "{{" + key + "}}"
            if isinstance(value, str):
                result = result.replace(placeholder, value)
            else:
                # For non-string values, convert to JSON string first
                result = result.replace(placeholder, json.dumps(value))
        return json.loads(result)


class TemplateRegistry:
    """Registry to manage all templates."""
    
    def __init__(self):
        self.templates: Dict[str, Template] = {}
    
    def register(self, name: str, template_str: str) -> Template:
        """Register a new template."""
        template = Template(template_str)
        self.templates[name] = template
        return template
    
    def get(self, name: str) -> Optional[Template]:
        """Get a template by name."""
        return self.templates.get(name)
    
    def render(self, name: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Render a template by name with the given parameters."""
        template = self.get(name)
        if not template:
            raise ValueError(f"Template '{name}' not found")
        return template.render(params)


# Create a global registry instance
registry = TemplateRegistry()


# Register all templates
def register_default_templates():
    """Register all default templates."""
    
    # Document template
    registry.register("document", """
    {
        "passage_embedding": {{embedding}}
    }
    """)
    
    # Index template
    registry.register("index", """
    {
        "settings": {
            "index": {
                "sparse": true,
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "sparse.memory": true
            }
        },
        "mappings": {
            "properties": {
                "passage_embedding":{
                    "type": "sparse_tokens",
                    "method": {
                        "name": "seismic",
                        "lambda": {{lambda}},
                        "alpha": {{alpha}},
                        "beta": {{beta}},
                        "cluster_until_doc_count_reach": {{doc_reach}}
                    }
                }
            }
        }
    }
    """)
    
    # Sparse query template
    registry.register("sparse_query", """
    {  
        "_source": {
            "excludes": [
                "sparse_embedding"
            ]
        },
        "query": {
            "neural_sparse": {
                "sparse_embedding": {
                    "query_tokens": {{embedding}}
                }
            }
        },
        "size": 10
    }
    """)
    
    # Sparse ANN query template
    registry.register("sparse_ann_query", """
    {
        "_source": false,
        "query": {
            "sparse_ann": {
                "passage_embedding": {
                    "query_tokens": {{embedding}},
                    "cut": {{cut}},
                    "heap_factor": {{hf}}
                }
            }
        },
        "size": 10
    }
    """)


# Initialize the registry with default templates
register_default_templates()