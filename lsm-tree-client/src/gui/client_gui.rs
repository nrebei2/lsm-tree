use std::thread;

use relm4::{
    channel,
    gtk::{
        self,
        glib::subclass::types::ObjectSubclassIsExt,
        prelude::{ButtonExt, OrientableExt, WidgetExt},
    },
    ComponentParts, ComponentSender, Sender, SimpleComponent,
};

use crate::{command::CommandType, run_gui_client};

use super::{command_panel::CommandPanelOutput, plot};

pub struct ClientGui {
    command_channel_sender: Sender<CommandPanelOutput>,
    plot: Option<plot::Plot>,
}

impl ClientGui {
    fn with_sender(sender: ComponentSender<Self>) -> Self {
        let (command_channel_sender, command_channel_recv) = channel();
        thread::spawn(move || {
            let _ = run_gui_client(sender, command_channel_recv);
        });
        Self {
            command_channel_sender,
            plot: None,
        }
    }
}

#[derive(Debug)]
pub enum ClientInput {
    FromCommandPanel(CommandPanelOutput),
    NewData(Box<[f32]>, Box<[CommandType]>),
    CommandCompleted,
    ClearGraph,
}

#[derive(Debug)]
pub enum ClientOutput {
    CommandCompleted,
}

#[relm4::component(pub)]
impl SimpleComponent for ClientGui {
    type Init = ();
    type Input = ClientInput;
    type Output = ClientOutput;

    view! {
        #[root]
        gtk::Box {
            set_orientation: gtk::Orientation::Vertical,

            #[name = "plot"]
            plot::Plot {
                set_height_request: 500,
            },

            gtk::Button {
                set_label: "Clear",
                connect_clicked => ClientInput::ClearGraph
            },
        }
    }

    fn init(_init: (), root: Self::Root, sender: ComponentSender<Self>) -> ComponentParts<Self> {
        let mut model = ClientGui::with_sender(sender.clone());
        let widgets = view_output!();

        model.plot = Some(widgets.plot.clone());
        ComponentParts { model, widgets }
    }

    fn update(&mut self, msg: Self::Input, sender: ComponentSender<Self>) {
        match msg {
            ClientInput::FromCommandPanel(cpo) => {
                self.command_channel_sender.emit(cpo);
            }
            ClientInput::NewData(data, command_types) => {
                // update and redraw plot
                let plot_widget = self.plot.as_ref().unwrap();
                let plot = plot_widget.imp();
                plot.data.borrow_mut().push(data, command_types);
                plot_widget.queue_draw();
            }
            ClientInput::CommandCompleted => {
                let _ = sender.output(ClientOutput::CommandCompleted);
            }
            ClientInput::ClearGraph => {
                let plot_widget = self.plot.as_ref().unwrap();
                let plot = plot_widget.imp();
                plot.data.borrow_mut().clear();
                plot_widget.queue_draw();
            }
        }
    }
}
