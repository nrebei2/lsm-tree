use gtk::glib;
use relm4::gtk;

mod imp;

glib::wrapper! {
    pub struct Plot(ObjectSubclass<imp::Plot>) @extends gtk::Widget;
}

impl Default for Plot {
    fn default() -> Self {
        glib::object::Object::new::<Self>()
    }
}