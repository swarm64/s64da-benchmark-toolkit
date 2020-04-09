{% extends "html.tpl" %}
{% block table %}
<style type="text/css">
    th {
          background-color: darkorange;
          color: white;
          border: 1px solid black;
    }

    td {
            border: 1px solid black;
    }

    tr:nth-child(even) {
            background-color: #F2F2F2;
    }
</style>

<h2>{{ table_title }}</h2>
{{ super() }}
{% endblock table %}
