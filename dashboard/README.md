# Dashboard Power BI - AFRIDATA PIPELINE

## Connexion des données

1. Power BI Desktop -> **Obtenir les données** -> PostgreSQL
2. Serveur : `localhost:5432`, base : `warehouse`
3. Importer les vues : `v_kpi_overview`, `v_daily_evolution`, `v_operator_performance`,
   `v_region_map`, `v_suspect_list`, `v_hourly_activity`, `v_daily_failures`.
4. Créer une mesure DAX de base :

```DAX
Taux d echec % = DIVIDE(SUM(v_kpi_overview[total_failed]), SUM(v_kpi_overview[total_transactions]), 0)
```

## Pages du dashboard

### Page 1 — Vue d'ensemble
| Visuel | Donnée |
| ------ | ------ |
| Carte KPI | Volume total de transactions (`total_transactions`) |
| Carte KPI | Montant total transféré (`total_amount`) + total des frais |
| Carte KPI | Taux d'échec (`fail_rate`) — mise en forme conditionnelle (> 5 % en rouge) |
| Anneau | Transactions par opérateur (`v_operator_performance.market_share`) |
| Courbe | Évolution quotidienne (`v_daily_evolution`, axe tx_date) |

### Page 2 — Anomalies
| Visuel | Donnée |
| ------ | ------ |
| Carte KPI | Nombre de transactions suspectes (`fct_suspect_transactions`) |
| Histogramme empilé | Suspects par jour et par règle (`agg_suspects`) |
| Nuage de points | `if_score` vs `amount`, couleur = `if_label` |
| Table | Détail des suspects (`v_suspect_list`) |

### Page 3 — Comportement des utilisateurs
| Visuel | Donnée |
| ------ | ------ |
| Heatmap | Heure x canal (`v_hourly_activity`) |
| Carte choroplèthe | Répartition régionale (`v_region_map` — régions de Côte d'Ivoire) |
| Barres empilées | Types de transaction par tranche de montant (`agg_tx_type_channel`) |
| Courbe | Alertes taux d'échec (`v_daily_failures`) |

## Rendu attendu

Placer les captures d'écran du dashboard dans `dashboard/img/` :
- `img/page1_overview.png`
- `img/page2_anomalies.png`
- `img/page3_behavior.png`
