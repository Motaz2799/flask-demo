import requests
import networkx as nx
import matplotlib.pyplot as plt

def get_interface(app_id):
    auth_url = 'http://localhost:8080/api/v1/auth/authenticate'
    auth_payload = {'email': 'archimaster@orange.com', 'password': '123'}
    auth_response = requests.post(auth_url, json=auth_payload)
    if auth_response.status_code == 200:
        token = auth_response.json().get('token')
    else:
        return 'Failed to authenticate'

    headers = {'Authorization': f'Bearer {token}'}
    response = requests.get('http://127.0.0.1:8080/api/v1/interfaces', headers=headers)
    if response.status_code == 200:
        data = response.json()
    G = nx.Graph()
    app_name = ""
    for relation in data:
        if app_id == relation['applicationSrc']['id']:
            app_name = relation['applicationSrc']['appName']
        elif app_id == relation['applicationTarget']['id']:
            app_name = relation['applicationTarget']['appName']

    G.add_node(app_id, label=app_name, color='orange')

    for relation in data:
        source_app_id = relation['applicationSrc']['id']
        target_app_id = relation['applicationTarget']['id']
        protocol = relation['protocol']
        if target_app_id == app_id:
            source_app_name = relation['applicationSrc']['appName']
            G.add_node(source_app_id, label=source_app_name, color='lime')
            G.add_edge(source_app_id, app_id, color='lightgray', label=f'{protocol}')


        if source_app_id == app_id:
            target_app_name = relation['applicationTarget']['appName']
            G.add_node(target_app_id, label=target_app_name, color='skyblue')
            G.add_edge(app_id, target_app_id, color='lightgray', label=f'{protocol}')

    node_colors = [node[1]['color'] for node in G.nodes(data=True)]
    edge_colors = [edge[2]['color'] for edge in G.edges(data=True)]
    edge_labels = {edge[:2]: edge[2]['label'] for edge in G.edges(data=True)}

    # Créer la figure
    fig, ax = plt.subplots()
    pos = nx.spring_layout(G)  # Positionnement des nœuds
    nx.draw_networkx_nodes(G, pos, node_color=node_colors, node_size=1000)
    node_labels = nx.get_node_attributes(G, 'label')
    nx.draw_networkx_labels(G, pos, labels=node_labels)
    nx.draw_networkx_edges(G, pos, edge_color=edge_colors)
    nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels)

    plt.tight_layout()

   
    legend_colors = {'Application cible': 'orange', 'Application source': 'lime', 'Application destination': 'skyblue'}
    legend_markers = [plt.Line2D([0], [0], marker='o', color='w', markerfacecolor=color, markersize=10) for color in legend_colors.values()]
    legend_labels = list(legend_colors.keys())
    legend = plt.legend(legend_markers, legend_labels, loc='upper center', bbox_to_anchor=(0.5, -0.1), ncol=3)

    plt.axis('off')
    plt.subplots_adjust(bottom=0.2)  
    image_path = f"application{app_id}_interfaces.png"
    plt.savefig(image_path)
    return(image_path)
