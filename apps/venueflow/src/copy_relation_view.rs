use eframe::egui;
use venue_control_protocol::{CopyRelationRecord, CopyRelationSummary, StrategySummary};

use crate::{
    client::ControlClient,
    i18n::{Language, TextKey, text},
    model::{AppModel, CopyRelationDraft, decimal_to_f64, format_decimal},
    theme,
    ui::{empty, pane_heading},
};

pub(crate) fn show(ui: &mut egui::Ui, model: &mut AppModel, client: &ControlClient) {
    let language = model.preferences.language;
    pane_heading(
        ui,
        text(language, TextKey::CopyRelations),
        text(language, TextKey::CopySubtitle),
    );
    let Some(snapshot) = &model.snapshot else {
        empty(ui, text(language, TextKey::NoCopy));
        return;
    };
    let relations = snapshot.copy_relations.clone();
    let strategies = snapshot.strategies.clone();
    if ui.button("Create relation").clicked() {
        model.copy_relation_draft = Some(CopyRelationDraft::new());
    }
    if relations.is_empty() {
        empty(ui, text(language, TextKey::NoCopy));
        editor(ui, model, client);
        return;
    }
    egui::Grid::new("copy-grid").striped(true).show(ui, |ui| {
        for heading in [
            TextKey::Leader,
            TextKey::Follower,
            TextKey::Symbol,
            TextKey::Target,
            TextKey::Actual,
            TextKey::Drift,
            TextKey::State,
        ] {
            ui.strong(text(language, heading));
        }
        ui.end_row();
        for relation in &relations {
            let selected = model.preferences.selected_copy_relation.as_deref()
                == Some(relation.relation_id.as_str());
            if ui.selectable_label(selected, &relation.leader_id).clicked()
                || ui
                    .selectable_label(selected, &relation.follower_instance_id)
                    .clicked()
            {
                model.select_copy_relation(
                    &relation.relation_id,
                    &relation.follower_instance_id,
                    &relation.symbol.to_string(),
                );
            }
            ui.label(relation.symbol.to_string());
            ui.monospace(format_decimal(relation.target_exposure, 4));
            ui.monospace(format_decimal(relation.actual_exposure, 4));
            let drift = decimal_to_f64(relation.drift);
            ui.colored_label(theme::value_color(drift), format!("{drift:+.4}"));
            ui.label(format!("{:?}", relation.status));
            ui.end_row();
        }
    });
    if let Some(relation) = model
        .preferences
        .selected_copy_relation
        .as_deref()
        .and_then(|id| relations.iter().find(|row| row.relation_id == id).cloned())
    {
        let config = model
            .copy_relation_configs
            .iter()
            .find(|record| record.relation.relation_id == relation.relation_id)
            .cloned();
        detail(ui, model, language, &relation, &strategies, config);
    } else {
        ui.small(text(language, TextKey::SelectCopyRelation));
    }
    editor(ui, model, client);
}

fn detail(
    ui: &mut egui::Ui,
    model: &mut AppModel,
    language: Language,
    relation: &CopyRelationSummary,
    strategies: &[StrategySummary],
    config: Option<CopyRelationRecord>,
) {
    ui.separator();
    ui.strong(text(language, TextKey::CopyRelationDetails));
    ui.label(format!(
        "{}: {} · {}: {} · {}",
        text(language, TextKey::Leader),
        relation.leader_id,
        text(language, TextKey::FollowerInstance),
        relation.follower_instance_id,
        relation.symbol
    ));
    ui.monospace(format!(
        "relation_id={} · revision={}",
        relation.relation_id, relation.revision
    ));
    ui.horizontal_wrapped(|ui| {
        metric(
            ui,
            text(language, TextKey::Target),
            relation.target_exposure,
        );
        metric(
            ui,
            text(language, TextKey::Actual),
            relation.actual_exposure,
        );
        metric(ui, text(language, TextKey::Drift), relation.drift);
    });
    ui.label(format!(
        "{}: {:?}",
        text(language, TextKey::RelationStatus),
        relation.status
    ));
    ui.label(format!(
        "{}: {}",
        text(language, TextKey::LastAppliedJob),
        relation
            .last_applied_job
            .as_deref()
            .unwrap_or(text(language, TextKey::NoAppliedJob))
    ));
    if let Some(follower) = strategies
        .iter()
        .find(|row| row.instance_id == relation.follower_instance_id)
    {
        ui.label(format!(
            "Follower: {} · {} · {} · {}",
            follower.venue, follower.trading_account_id, follower.instance_id, follower.symbol
        ));
    }
    if let Some(record) = config {
        let relation = record.relation;
        ui.strong(text(language, TextKey::CopyRelationConfiguration));
        ui.label(format!(
            "Leader: {} · LIVE · {} · {} · {}",
            relation.leader.venue,
            relation.leader.trading_account_id,
            relation.leader.instance_id,
            relation.leader.symbol
        ));
        ui.label(format!("capital={} multiplier={} reserve={} max_total={} max_order={} leverage={} lifecycle={:?}", format_decimal(relation.allocated_capital, 4), format_decimal(relation.multiplier, 4), format_decimal(relation.safety_reserve_rate, 4), format_decimal(relation.risk.max_total_notional, 4), format_decimal(relation.risk.max_order_notional, 4), format_decimal(relation.risk.max_leverage, 4), relation.lifecycle));
        if ui.button("Edit relation").clicked() {
            model.copy_relation_draft =
                Some(CopyRelationDraft::from_config(&relation, record.revision));
        }
    } else {
        ui.colored_label(theme::WARNING, "Configuration projection is still loading.");
    }
}

fn metric(ui: &mut egui::Ui, label: &str, value: rust_decimal::Decimal) {
    ui.label(format!("{label}: {}", format_decimal(value, 4)));
}

fn editor(ui: &mut egui::Ui, model: &mut AppModel, client: &ControlClient) {
    let Some(mut draft) = model.copy_relation_draft.take() else {
        return;
    };
    ui.separator();
    ui.strong(if draft.expected_revision.is_some() {
        "Edit copy relation"
    } else {
        "Create copy relation"
    });
    ui.small("Configuration is submitted to Control only; VenueFlow has no writer, credential, or exchange client.");
    field(ui, "Relation ID (UUID)", &mut draft.relation_id);
    binding(
        ui,
        "Leader",
        &mut draft.leader_venue,
        &mut draft.leader_account_id,
        &mut draft.leader_instance_id,
        &mut draft.leader_symbol,
    );
    binding(
        ui,
        "Follower",
        &mut draft.follower_venue,
        &mut draft.follower_account_id,
        &mut draft.follower_instance_id,
        &mut draft.follower_symbol,
    );
    for (label, value) in [
        ("Allocated capital", &mut draft.allocated_capital),
        ("Multiplier", &mut draft.multiplier),
        ("Safety reserve rate [0,1)", &mut draft.safety_reserve_rate),
        ("Maximum total notional", &mut draft.max_total_notional),
        ("Maximum order notional", &mut draft.max_order_notional),
        ("Maximum leverage", &mut draft.max_leverage),
    ] {
        field(ui, label, value);
    }
    egui::ComboBox::from_id_salt("copy-relation-lifecycle")
        .selected_text(format!("{:?}", draft.lifecycle))
        .show_ui(ui, |ui| {
            ui.selectable_value(
                &mut draft.lifecycle,
                venue_control_protocol::CopyLifecyclePolicy::Active,
                "Active",
            );
            ui.selectable_value(
                &mut draft.lifecycle,
                venue_control_protocol::CopyLifecyclePolicy::Paused,
                "Paused",
            );
        });
    let mut keep = true;
    ui.horizontal(|ui| {
        if ui.button("Cancel").clicked() {
            keep = false;
        }
        if ui.button("Save relation").clicked() {
            match draft.to_request() {
                Ok(request) => match client.send_copy_relation(request) {
                    Ok(()) => {
                        model.notice("Copy relation configuration submitted to Control");
                        keep = false;
                    }
                    Err(error) => {
                        model.notice(format!("Copy relation request rejected locally: {error}"))
                    }
                },
                Err(error) => model.notice(error),
            }
        }
    });
    if keep {
        model.copy_relation_draft = Some(draft);
    }
}

fn binding(
    ui: &mut egui::Ui,
    name: &str,
    venue: &mut venue_control_protocol::VenueId,
    account: &mut String,
    instance: &mut String,
    symbol: &mut String,
) {
    ui.strong(format!("{name} binding"));
    egui::ComboBox::from_id_salt(format!("{name}-venue"))
        .selected_text(venue.to_string())
        .show_ui(ui, |ui| {
            for candidate in venue_control_protocol::VenueId::ALL {
                ui.selectable_value(venue, candidate, candidate.to_string());
            }
        });
    ui.monospace("LIVE");
    field(ui, "Trading account ID", account);
    field(ui, "Instance ID", instance);
    field(ui, "Symbol", symbol);
}

fn field(ui: &mut egui::Ui, label: &str, value: &mut String) {
    ui.horizontal(|ui| {
        ui.label(label);
        ui.text_edit_singleline(value);
    });
}
