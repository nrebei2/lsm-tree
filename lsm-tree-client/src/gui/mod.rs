use client_gui::{ClientGui, ClientInput};
use command_panel::{CommandPanel, CommandPanelOutput};
use relm4::{
    gtk::{self, prelude::*},
    Component, ComponentController, ComponentParts, ComponentSender, Controller, RelmWidgetExt,
    SimpleComponent,
};

pub mod client_gui;
pub mod command_panel;
pub mod plot;

pub struct App {
    command_panel: Controller<CommandPanel>,
    client_vis: Controller<ClientGui>,
}

#[derive(Debug)]
pub enum AppInput {
    FromCommandPanel(CommandPanelOutput),
    CommandCompleted,
}

#[relm4::component(pub)]
impl SimpleComponent for App {
    type Init = ();
    type Input = AppInput;
    type Output = ();

    view! {
        gtk::Window {
            set_title: Some("Client Interface"),
            set_default_width: 1400,
            set_default_height: 800,

            #[name = "top_box"]
            gtk::Box {
                set_orientation: gtk::Orientation::Vertical,
                set_spacing: 5,
                set_margin_all: 5,
            }
        }
    }

    fn init(_init: (), root: Self::Root, sender: ComponentSender<Self>) -> ComponentParts<Self> {
        let widgets = view_output!();

        let command_panel = CommandPanel::builder()
            .attach_to(&widgets.top_box)
            .launch(())
            .forward(sender.input_sender(), AppInput::FromCommandPanel);

        let client_vis = ClientGui::builder()
            .attach_to(&widgets.top_box)
            .launch(())
            .forward(sender.input_sender(), |_| AppInput::CommandCompleted);

        let model = App {
            command_panel,
            client_vis,
        };

        ComponentParts { model, widgets }
    }

    fn update(&mut self, msg: Self::Input, _sender: ComponentSender<Self>) {
        match msg {
            AppInput::FromCommandPanel(cpm) => {
                let _ = self
                    .client_vis
                    .sender()
                    .send(ClientInput::FromCommandPanel(cpm));
            }
            AppInput::CommandCompleted => {
                let _ = self
                    .command_panel
                    .sender()
                    .send(command_panel::CommandPanelMsg::CommandCompleted);
            }
        }
    }
}
