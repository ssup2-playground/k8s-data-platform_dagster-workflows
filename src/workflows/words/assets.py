from dagster import asset

@asset(key_prefix=["examples"], 
    group_name="words",
    description="Generate apple, banana, cherry words", 
    kinds=["python"],
    op_tags={
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "2000m", "memory": "4096Mi"},
                }
            },
        }
    })
def generated_fruits_words():
    return ["apple", "banana", "cherry"]

@asset(key_prefix=["examples"], 
    group_name="words",
    description="Generate dog, cat, bird words", 
    kinds=["python"],
    op_tags={
        "dagster-k8s/config": {
            "container_config": { 
                "resources": {
                    "requests": {"cpu": "2000m", "memory": "4096Mi"},
                }
            },
        }
    })
def generated_animals_words():
    return ["dog", "cat", "bird"]

@asset(key_prefix=["examples"], 
    group_name="words",
    description="Sum the two lists of words", 
    kinds=["python"],
    op_tags={
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "2000m", "memory": "4096Mi"},
                }
            },
        }
    }) 
def summed_words(generated_fruits_words, generated_animals_words):
    return [generated_fruits_words, generated_animals_words]