use std::collections::HashMap;

use relm4::{
    gtk::{self, prelude::*, Entry, SpinButton},
    ComponentParts, ComponentSender, RelmWidgetExt, SimpleComponent,
};

#[derive(Debug, Clone, PartialEq)]
enum SelectedCommand {
    GeneratePuts,
    GenerateWorkload,
    RawCommand,
}

#[derive(Debug)]
pub enum CommandPanelMsg {
    SetCommand(u32),
    Send,
    CommandCompleted,
}

#[derive(Debug)]
pub enum CommandPanelOutput {
    GeneratePuts {
        num_puts: u32,
    },
    GenerateWorkload {
        num_puts: u32,
        num_gets: u32,
        gets_skew: f64,
        gets_miss_ratio: f64,
        num_ranges: u32,
        num_deletes: u32,
    },
    RawCommand {
        command: String,
    },
}

#[derive(Debug)]
pub struct CommandPanel {
    selected: SelectedCommand,
    spin_button_widgets: HashMap<&'static str, SpinButton>,
    entry_widgets: HashMap<&'static str, Entry>,
    busy: bool,
}

#[relm4::component(pub)]
impl SimpleComponent for CommandPanel {
    type Init = ();
    type Input = CommandPanelMsg;
    type Output = CommandPanelOutput;

    view! {
        #[root]
        gtk::Box {
            set_orientation: gtk::Orientation::Horizontal,
            set_spacing: 10,
            set_margin_all: 10,
            set_homogeneous: true,

            // DropDown to choose command
            gtk::DropDown::from_strings(&["Generate Load", "Generate Workload", "Raw Command"]) {
                set_vexpand: false,
                connect_selected_notify[sender] => move |dropdown| {
                    sender.input(CommandPanelMsg::SetCommand(dropdown.selected()))
                },
            },

            // Conditionally rendered content based on selection
            // #[track = "model.selected.clone()"]
            match &model.selected {
                SelectedCommand::GeneratePuts => {
                    gtk::Box {
                        set_orientation: gtk::Orientation::Vertical,
                        gtk::Label {
                            set_label: "number of puts",
                        },

                        #[name = "gen_puts"]
                        gtk::SpinButton {
                            set_range: (0.0, 100000000.0),
                            set_increments: (1.0, 10.0),
                        }
                    }
                },
                SelectedCommand::GenerateWorkload => {
                    gtk::Box {
                        set_orientation: gtk::Orientation::Horizontal,
                        set_homogeneous: true,
                        set_spacing: 10,

                        gtk::Box {
                            set_orientation: gtk::Orientation::Vertical,
                            gtk::Label { set_label: "number of puts" },
                            #[name = "work_puts"]
                            gtk::SpinButton { set_range: (0.0, 100000000.0), set_increments: (1.0, 10.0) },

                            gtk::Label { set_label: "number of gets" },
                            #[name = "work_gets"]
                            gtk::SpinButton { set_range: (0.0, 100000000.0), set_increments: (1.0, 10.0) },

                            gtk::Label { set_label: "gets-skewness" },
                            #[name = "work_gs"]
                            gtk::SpinButton {
                                set_range: (0.0, 1.0),
                                set_increments: (0.1, 1.0),
                                set_digits: 3
                            },
                        },

                        gtk::Box {
                            set_orientation: gtk::Orientation::Vertical,
                            gtk::Label { set_label: "gets-misses-ratio" },
                            #[name = "work_gmr"]
                            gtk::SpinButton {
                                set_range: (0.0, 1.0),
                                set_increments: (0.1, 1.0),
                                set_digits: 3
                            },

                            gtk::Label { set_label: "number of ranges" },
                            #[name = "work_ranges"]
                            gtk::SpinButton { set_range: (0.0, 100000000.0), set_increments: (1.0, 10.0) },

                            gtk::Label { set_label: "number of deletes" },
                            #[name = "work_deletes"]
                            gtk::SpinButton { set_range: (0.0, 100000000.0), set_increments: (1.0, 10.0) },
                        }
                    }
                },
                SelectedCommand::RawCommand => {
                    #[name = "raw_text"]
                    gtk::Entry {}
                }
            },

            gtk::Box {
                set_orientation: gtk::Orientation::Vertical,
                set_homogeneous: true,
                set_spacing: 10,

                gtk::Button {
                    set_label: "Send",
                    connect_clicked => CommandPanelMsg::Send
                },
                gtk::Spinner {
                    #[watch]
                    set_spinning: model.busy,
                }
            }
        },
    }

    fn init(_init: (), root: Self::Root, sender: ComponentSender<Self>) -> ComponentParts<Self> {
        let mut model = CommandPanel {
            selected: SelectedCommand::GeneratePuts,
            spin_button_widgets: HashMap::new(),
            entry_widgets: HashMap::new(),
            busy: false,
        };

        let widgets = view_output!();

        model
            .spin_button_widgets
            .insert("gen_puts", widgets.gen_puts.clone());
        model
            .spin_button_widgets
            .insert("work_puts", widgets.work_puts.clone());
        model
            .spin_button_widgets
            .insert("work_gets", widgets.work_gets.clone());
        model
            .spin_button_widgets
            .insert("work_ranges", widgets.work_ranges.clone());
        model
            .spin_button_widgets
            .insert("work_deletes", widgets.work_deletes.clone());
        model
            .spin_button_widgets
            .insert("work_gs", widgets.work_gs.clone());
        model
            .spin_button_widgets
            .insert("work_gmr", widgets.work_gmr.clone());

        model
            .entry_widgets
            .insert("raw_text", widgets.raw_text.clone());

        ComponentParts { model, widgets }
    }

    fn update(&mut self, msg: Self::Input, _sender: ComponentSender<Self>) {
        match msg {
            CommandPanelMsg::SetCommand(0) => self.selected = SelectedCommand::GeneratePuts,
            CommandPanelMsg::SetCommand(1) => self.selected = SelectedCommand::GenerateWorkload,
            CommandPanelMsg::SetCommand(2) => self.selected = SelectedCommand::RawCommand,
            CommandPanelMsg::Send => {
                if self.busy {
                    return;
                }
                match self.selected {
                    SelectedCommand::GeneratePuts => {
                        let _ = _sender.output(CommandPanelOutput::GeneratePuts {
                            num_puts: self.get_spin_val("gen_puts"),
                        });
                    }
                    SelectedCommand::GenerateWorkload => {
                        let _ = _sender.output(CommandPanelOutput::GenerateWorkload {
                            num_puts: self.get_spin_val("work_puts"),
                            num_gets: self.get_spin_val("work_gets"),
                            gets_skew: self.get_spin_float("work_gs"),
                            gets_miss_ratio: self.get_spin_float("work_gmr"),
                            num_ranges: self.get_spin_val("work_ranges"),
                            num_deletes: self.get_spin_val("work_deletes"),
                        });
                    }
                    SelectedCommand::RawCommand => {
                        let _ = _sender.output(CommandPanelOutput::RawCommand {
                            command: self.entry_widgets.get("raw_text").unwrap().text().into(),
                        });
                    }
                }
                self.busy = true;
            }
            CommandPanelMsg::CommandCompleted => {
                self.busy = false;
            }
            _ => {}
        }
    }
}

impl CommandPanel {
    fn get_spin_val(&self, name: &'static str) -> u32 {
        self.spin_button_widgets.get(name).unwrap().value_as_int() as u32
    }

    fn get_spin_float(&self, name: &'static str) -> f64 {
        self.spin_button_widgets.get(name).unwrap().value()
    }
}
