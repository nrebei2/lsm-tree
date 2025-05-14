use gtk::glib;
use gtk::prelude::*;
use gtk::subclass::prelude::*;
use relm4::gtk;

use std::cell::RefCell;
use std::error::Error;

use plotters::prelude::*;
use plotters_cairo::CairoBackend;

use crate::command::CommandType;

#[derive(Default, glib::Properties)]
#[properties(wrapper_type = super::Plot)]
pub struct Plot {
    pub data: RefCell<PlotData>,
    pub command_types: RefCell<Vec<CommandType>>,
}

#[derive(Debug)]
pub struct PlotData {
    puts: Vec<(u32, f32)>,
    gets: Vec<(u32, f32)>,
    ranges: Vec<(u32, f32)>,
    deletes: Vec<(u32, f32)>,
    min: f32,
    max: f32,
    total: u32,
}

impl Default for PlotData {
    fn default() -> Self {
        Self {
            puts: Vec::new(),
            gets: Vec::new(),
            ranges: Vec::new(),
            deletes: Vec::new(),
            min: f32::INFINITY,
            max: 0.0,
            total: 0,
        }
    }
}

impl PlotData {
    pub fn clear(&mut self) {
        self.puts.clear();
        self.gets.clear();
        self.ranges.clear();
        self.deletes.clear();
        self.min = f32::INFINITY;
        self.max = 0.0;
        self.total = 0;
    }

    pub fn push(&mut self, data: Box<[f32]>, c_types: Box<[CommandType]>) {
        for (c_type, y) in c_types.iter().zip(data) {
            let vec = match *c_type {
                CommandType::PUT => &mut self.puts,
                CommandType::GET => &mut self.gets,
                CommandType::RANGE => &mut self.ranges,
                CommandType::DELETE => &mut self.deletes,
            };

            vec.push((self.total, y));
            self.total += 1;

            self.max = self.max.max(y);
            self.min = self.min.min(y);
        }
        self.downsample(2000);
    }

    fn downsample(&mut self, threshold: usize) {
        Self::run_lttb(&mut self.gets, threshold);
        Self::run_lttb(&mut self.puts, threshold);
        Self::run_lttb(&mut self.ranges, threshold);
        Self::run_lttb(&mut self.deletes, threshold);
    }

    // https://skemman.is/bitstream/1946/15343/3/SS_MSthesis.pdf
    fn run_lttb(data: &mut Vec<(u32, f32)>, threshold: usize) {
        // only run if more than 3 times the threshold
        if threshold * 3 >= data.len() || threshold == 0 {
            return;
        }

        let spread = data[data.len() - 1].0 - data[0].0;

        // Bucket length in x-units
        let every = ((spread - 1) as f64) / ((threshold - 2) as f64);

        let mut cur_bucket_range = 1..data[1..]
            .iter()
            .position(|(x, _)| *x > (every as u32))
            .unwrap();

        let mut a = (0u32, data[0].1);

        for i in 0..threshold - 2 {
            let mut c_x = 0f64;
            let mut c_y = 0f64;

            let mut next_bucket_range = cur_bucket_range.end
                ..data[cur_bucket_range.end..]
                    .iter()
                    .position(|(x, _)| *x > (((i + 2) as f64) * every) as u32)
                    .map(|idx| idx + cur_bucket_range.end)
                    .unwrap_or(data.len());

            if next_bucket_range.start == next_bucket_range.end {
                next_bucket_range.end += 1;
            }

            let next_bucket_len = next_bucket_range.len() as f64;

            for idx in next_bucket_range.clone() {
                c_x += data[idx].0 as f64;
                c_y += data[idx].1 as f64;
            }

            c_x /= next_bucket_len;
            c_y /= next_bucket_len;

            let mut max_area = -1f64;
            let mut next_a_idx = cur_bucket_range.start;

            for idx in cur_bucket_range {
                let area = ((a.0 as f64 - c_x) * ((data[idx].1 - a.1) as f64)
                    - (a.0 as f64 - data[idx].0 as f64) * (c_y - a.1 as f64))
                    .abs()
                    * 0.5;

                if area > max_area {
                    max_area = area;
                    next_a_idx = idx;
                }
            }

            data[i + 1] = data[next_a_idx];
            a = data[next_a_idx];

            cur_bucket_range = next_bucket_range;
        }

        data[threshold - 1] = data[data.len() - 1];
        data.truncate(threshold);
    }
}

#[glib::object_subclass]
impl ObjectSubclass for Plot {
    const NAME: &'static str = "GaussianPlot";
    type Type = super::Plot;
    type ParentType = gtk::Widget;
}

impl ObjectImpl for Plot {
    fn properties() -> &'static [glib::ParamSpec] {
        Self::derived_properties()
    }

    fn set_property(&self, id: usize, value: &glib::Value, pspec: &glib::ParamSpec) {
        Self::derived_set_property(self, id, value, pspec);
        self.obj().queue_draw();
    }

    fn property(&self, id: usize, pspec: &glib::ParamSpec) -> glib::Value {
        Self::derived_property(self, id, pspec)
    }
}

impl WidgetImpl for Plot {
    fn snapshot(&self, snapshot: &gtk::Snapshot) {
        let width = self.obj().width() as u32;
        let height = self.obj().height() as u32;
        if width == 0 || height == 0 {
            return;
        }

        let bounds = gtk::graphene::Rect::new(0.0, 0.0, width as f32, height as f32);
        let cr = snapshot.append_cairo(&bounds);
        let backend = CairoBackend::new(&cr, (width, height)).unwrap();
        self.plot_pdf(backend).unwrap();
    }
}

impl Plot {
    fn plot_pdf<'a, DB: DrawingBackend + 'a>(
        &self,
        backend: DB,
    ) -> Result<(), Box<dyn Error + 'a>> {
        let root = backend.into_drawing_area();

        root.fill(&WHITE)?;

        let plot_data = self.data.borrow();

        let mut cc = ChartBuilder::on(&root)
            .margin(10)
            .caption("Latency", ("sans-serif", 30))
            .x_label_area_size(40)
            .y_label_area_size(50)
            .build_cartesian_2d(
                0..plot_data.total.saturating_sub(1),
                if plot_data.max == 0.0 {
                    0f32..1f32
                } else {
                    plot_data.min/5.0..plot_data.max*5.0
                }
                .log_scale(),
            )?;

        cc.configure_mesh()
            .x_labels(15)
            .y_labels(10)
            .x_desc("Command #")
            .y_desc("Seconds")
            .axis_desc_style(("sans-serif", 15))
            .draw()?;

        let axes = [
            (CommandType::PUT, &plot_data.puts, Palette99::pick(0)),
            (CommandType::GET, &plot_data.gets, Palette99::pick(1)),
            (CommandType::RANGE, &plot_data.ranges, Palette99::pick(2)),
            (CommandType::DELETE, &plot_data.deletes, Palette99::pick(3)),
        ];

        for (c_type, data, palette) in axes {
            cc.draw_series(LineSeries::new(data.iter().cloned(), &palette))?
                .label(format!("{:?}", c_type))
                .legend(move |(x, y)| Rectangle::new([(x - 5, y - 5), (x + 5, y + 5)], &palette));
        }

        cc.configure_series_labels()
            .background_style(&WHITE.mix(0.8))
            .border_style(&BLACK)
            .draw()?;

        root.present()?;
        Ok(())
    }
}
