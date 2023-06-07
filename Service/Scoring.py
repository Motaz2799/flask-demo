import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn import preprocessing
from flask import send_file
import matplotlib.pyplot as plt

def calculate_scores(app_id, detailed):
    engine = create_engine('mysql+pymysql://root:root@localhost/cra')
    df_assessment_response = pd.read_sql_table('assessment_response', engine)
    labels = ['revise', 'retain', 'rehost', 'rebuild', 'rearchitect', 'retire', 'replace']
    vectors = ['benefit', 'effort', 'risk']
    label_descriptions = {
        "revise": "The 'revise' migration strategy involves making significant modifications or adjustments to the existing system before migrating it to the cloud.",
        "retain": "The 'retain' migration strategy focuses on keeping the current system as it is, without any significant changes, while moving it to the cloud infrastructure.",
        "rehost": "The 'rehost' migration strategy, also known as 'lift and shift,' involves moving the existing system to the cloud without making significant changes to its architecture.",
        "rebuild": "The 'rebuild' migration strategy entails rebuilding the system from scratch using cloud-native technologies and architectures.",
        "rearchitect": "The 'rearchitect' migration strategy involves making significant architectural changes to the system to optimize it for cloud environments.",
        "retire": "The 'remain' migration strategy refers to keeping the system in its current state without migrating it to the cloud.",
        "replace": "The 'replace' migration strategy involves replacing the existing system with a new system or solution in the cloud."
    }
    df_options = pd.read_sql_table('options', engine)
    list_id_options = []
    filtered_df = df_assessment_response.loc[df_assessment_response['app_id'] == app_id]
    for index, row in filtered_df.iterrows():
        for _, df_op in df_options.iterrows():
            if row['response'] == df_op['option']:
                list_id_options.append(df_op['id'])
    excel_file = pd.ExcelFile('Ressources/score.xlsx')
    matrix = pd.DataFrame(0, index=labels, columns=vectors)

    for vector in vectors:
        data = pd.read_excel(excel_file, sheet_name=vector, index_col=0)

        # Ajouter les pondérations au DataFrame de la matrice
        for label in labels:
            column_sum = data[data.columns.intersection(list_id_options)].loc[label, :].sum()
            matrix.loc[label, vector] = column_sum

    normalized_matrix = preprocessing.normalize(matrix, norm='l2', axis=0)
    criteria = [0.5, 0.3, 0.2]
    weighted_normalized_matrix = normalized_matrix * criteria
    ideal_best = np.max(weighted_normalized_matrix, axis=0)
    ideal_worst = np.min(weighted_normalized_matrix, axis=0)
    distance_best = np.sqrt(np.sum((weighted_normalized_matrix - ideal_best) ** 2, axis=1))
    distance_worst = np.sqrt(np.sum((weighted_normalized_matrix - ideal_worst) ** 2, axis=1))
    scores = distance_worst / (distance_worst + distance_best)

    recommended_label = labels[np.argmax(scores)]

    if detailed == 1:
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(8, 10))

        # Diagramme à barres
        colors = plt.cm.viridis(scores)
        ax1.bar(labels, scores, color=colors)
        ax1.set_xlabel('Strategies')
        ax1.set_ylabel('Scores')
        ax1.set_title(f'Recommended strategy: {recommended_label}')
        ax1.set_xticklabels(labels, rotation='vertical')
        ax1.set_ylim([0, 1])
        ax1.tick_params(axis='x', which='both', length=0)

        # Diagramme circulaire
        vector_values = normalized_matrix[labels.index(recommended_label)]
        explode = [0.1] * len(vectors) 
        colors = ['#1f77b4', '#ff7f0e', '#2ca02c']
        ax2.pie(vector_values, labels=vectors, colors=colors, explode=explode, startangle=90, autopct='%1.1f%%')
        ax2.set_aspect('equal')
        ax2.set_title('Vector Values')

        plt.tight_layout()
        image_path = f"application{app_id}.png"
        plt.savefig(image_path)

        return send_file(image_path, mimetype='image/png')
    if detailed == 2:
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(8, 10))

        # Diagramme à barres
        colors = plt.cm.viridis(scores)
        ax1.bar(labels, scores, color=colors)
        ax1.set_xlabel('Strategies')
        ax1.set_ylabel('Scores')
        ax1.set_title(f'Recommended strategy: {recommended_label}')
        ax1.set_xticklabels(labels, rotation='vertical')
        ax1.set_ylim([0, 1])
        ax1.tick_params(axis='x', which='both', length=0)

        # Diagramme circulaire
        vector_values = normalized_matrix[labels.index(recommended_label)]
        explode = [0.1] * len(vectors) 
        colors = ['#1f77b4', '#ff7f0e', '#2ca02c']
        ax2.pie(vector_values, labels=vectors, colors=colors, explode=explode, startangle=90, autopct='%1.1f%%')
        ax2.set_aspect('equal')
        ax2.set_title('Vector Values')

        plt.tight_layout()
        image_path = f"application{app_id}.png"
        plt.savefig(image_path)

        return {recommended_label: label_descriptions[recommended_label]}, image_path

    return {recommended_label: label_descriptions[recommended_label]}
