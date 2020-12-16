{% macro ref(model_name) %}
    {# if we are in unit tests we can pass a variable with the name replacement_MODELNAME #}
    {# and the value as the target model we want to replace it with #}
    {% if target.name == 'dev' %}
        {% if var('ref_' ~ model_name, None) %}
            {% set replaced_model_name = var('ref_' ~ model_name) ~ "_" ~ model_name %}
        {% else %}
            {% set replaced_model_name = model_name %}
        {% endif %}
        {% do return(builtins.ref(replaced_model_name)) %}
    {% else %}
        {% do return(builtins.ref(model_name)) %}
    {% endif %}
{% endmacro %}
