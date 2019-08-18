import React, { Component } from "react";
import "./App.css";
import "bootstrap/dist/css/bootstrap.css";
import DatePicker from "react-datepicker";
import "react-datepicker/dist/react-datepicker.css";
import "react-datepicker/dist/react-datepicker.css";
import Button from "react-bootstrap/Button";
import Jumbotron from "react-bootstrap/Jumbotron";
import Form from "react-bootstrap/Form";
import Container from "react-bootstrap/Container";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Card from "react-bootstrap/Card";
import Accordion from "react-bootstrap/Accordion";

class EntryForm extends React.Component {
  constructor(props) {
    super(props);

    this.state = {
      result: props.result,
      callback: props.callback,
      startDate: null,
      endDate: null,
      focusedInput: null,
      flyFrom: "NYC",
      flyTo: "CTG",
      exclusions: "",
      daysMinInt1: 2,
      daysMaxInt1: 4,
      daysMinInt2: 2,
      daysMaxInt2: 4,
      daysMinTarget: 4,
      daysMaxTarget: 10
    };

    this.handleInputChange = this.handleInputChange.bind(this);
    this.handleChangeStart = this.handleChangeStart.bind(this);
    this.handleChangeEnd = this.handleChangeEnd.bind(this);
  }

  async handleSubmit(event) {
    console.log("form submit");
    event.preventDefault();

    try {
      this.btn.setAttribute("disabled", "disabled");
      this.props.callback("loading");

      let dtFrom = new Date(this.state.startDate)
        .toISOString()
        .substring(0, 10);
      let dtTo = new Date(this.state.endDate).toISOString().substring(0, 10);

      let data = {
        flyFrom: this.state.flyFrom,
        flyTo: this.state.flyTo,
        dateFrom: dtFrom,
        dateTo: dtTo,
        exclusions: this.state.exclusions,
        daysMinInt1: this.state.daysMinInt1,
        daysMaxInt1: this.state.daysMaxInt1,
        daysMinInt2: this.state.daysMinInt2,
        daysMaxInt2: this.state.daysMaxInt2,
        daysMinTarget: this.state.daysMinTarget,
        daysMaxTarget: this.state.daysMaxTarget
      };

      console.log(data);

      const response = await fetch("http://localhost:5003/recos", {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json"
        },
        body: JSON.stringify(data)
      });

      const json = await response.json();

      console.log(json);

      this.props.callback(json);
    } catch (err) {
      this.props.callback("exception");
    } finally {
      this.btn.removeAttribute("disabled");
    }
  }

  handleInputChange(event) {
    console.log([event.target.name] + ":" + event.target.value);
    this.setState({
      [event.target.name]: event.target.value
    });
  }

  handleChangeEnd(date) {
    this.setState({
      endDate: date
    });
  }

  handleChangeStart(date) {
    this.setState({
      startDate: date
    });
  }

  render() {
    return (
      <Container>
        <Row>
          <Jumbotron fluid="true">
            <Container>
              <Row>
                <Col xs lg>
                  <Form
                    style={{
                      textAlign: "left"
                    }}
                    onSubmit={e => this.handleSubmit(e)}
                  >
                    <Accordion>
                      <Card style={{ overflow: "visible" }}>
                        <Card.Header style={{ textAlign: "left" }}>
                          Flights
                        </Card.Header>
                        <Card.Body>
                          <Form.Row>
                            <Col>
                              <Form.Group controlId="from">
                                <Form.Label>From</Form.Label>
                                <Form.Control
                                  type="text"
                                  name="flyFrom"
                                  value={this.state.flyFrom}
                                  onChange={this.handleInputChange}
                                  placeholder="NYC"
                                />
                              </Form.Group>
                            </Col>
                            <Col>
                              <Form.Group controlId="to">
                                <Form.Label>To</Form.Label>
                                <Form.Control
                                  name="flyTo"
                                  value={this.state.FlyTo}
                                  type="text"
                                  onChange={this.handleInputChange}
                                  placeholder="CTG"
                                />
                              </Form.Group>
                            </Col>
                          </Form.Row>

                          <Form.Row>
                            <Col>
                              <Form.Group controlId="dtFrom">
                                <Form.Label>Earliest Departure</Form.Label>
                                <DatePicker
                                  className="form-control"
                                  selected={this.state.startDate}
                                  selectsStart
                                  startDate={this.state.startDate}
                                  endDate={this.state.endDate}
                                  onChange={this.handleChangeStart}
                                  style={{ overflow: "visible" }}
                                />
                              </Form.Group>
                            </Col>
                            <Col>
                              <Form.Group controlId="dtTo">
                                <Form.Label>Latest Return</Form.Label>
                                <DatePicker
                                  className="form-control"
                                  selected={this.state.endDate}
                                  selectsEnd
                                  startDate={this.state.startDate}
                                  endDate={this.state.endDate}
                                  onChange={this.handleChangeEnd}
                                  minDate={this.state.startDate}
                                />
                              </Form.Group>
                            </Col>
                          </Form.Row>
                        </Card.Body>
                      </Card>
                      <Card>
                        <Accordion.Toggle
                          as={Button}
                          variant="link"
                          eventKey="1"
                          style={{ textAlign: "left" }}
                        >
                          Advanced
                        </Accordion.Toggle>
                        <Accordion.Collapse eventKey="1">
                          <Card.Body>
                            <Form.Row>
                              <Col>
                                <Form.Label>First Stop Length</Form.Label>
                              </Col>
                            </Form.Row>
                            <Form.Row>
                              <Col>
                                <Form.Control
                                  type="text"
                                  name="daysMinInt1"
                                  value={this.state.daysMinInt1}
                                  onChange={this.handleInputChange}
                                  defaultValue="2"
                                />
                              </Col>
                              <Col>
                                <Form.Group>
                                  <Form.Control
                                    type="text"
                                    name="daysMaxInt1"
                                    value={this.state.daysMaxInt1}
                                    onChange={this.handleInputChange}
                                    defaultValue="4"
                                  />
                                </Form.Group>
                              </Col>
                            </Form.Row>
                            <Form.Row>
                              <Col>
                                <Form.Label>Target Stop Length</Form.Label>
                              </Col>
                            </Form.Row>
                            <Form.Row>
                              <Col>
                                <Form.Control
                                  type="text"
                                  name="daysMinTarget"
                                  value={this.state.daysMinTarget}
                                  onChange={this.handleInputChange}
                                  defaultValue="6"
                                />
                              </Col>
                              <Col>
                                <Form.Group>
                                  <Form.Control
                                    type="text"
                                    name="daysMaxTarget"
                                    value={this.state.daysMaxTarget}
                                    onChange={this.handleInputChange}
                                    defaultValue="10"
                                  />
                                </Form.Group>
                              </Col>
                            </Form.Row>
                            <Form.Row>
                              <Col>
                                <Form.Label>Last Stop Length</Form.Label>
                              </Col>
                            </Form.Row>
                            <Form.Row>
                              <Col>
                                <Form.Control
                                  type="text"
                                  name="daysMinInt2"
                                  value={this.state.daysMinInt2}
                                  onChange={this.handleInputChange}
                                  defaultValue="2"
                                />
                              </Col>
                              <Col>
                                <Form.Group>
                                  <Form.Control
                                    type="text"
                                    name="daysMaxInt2"
                                    value={this.state.daysMaxInt2}
                                    onChange={this.handleInputChange}
                                    defaultValue="4"
                                  />
                                </Form.Group>
                              </Col>
                            </Form.Row>
                          </Card.Body>
                        </Accordion.Collapse>
                      </Card>
                      <Card>
                        <Card.Body style={{ textAlign: "center" }}>
                          <Button
                            variant="primary"
                            type="submit"
                            ref={btn => {
                              this.btn = btn;
                            }}
                          >
                            Submit
                          </Button>
                        </Card.Body>
                      </Card>
                    </Accordion>
                  </Form>
                </Col>
                <Col>
                  <h1>Trip Recommendations</h1>
                  <p>
                    Stop over for several days in another city or country for
                    cheaper than direct flights
                  </p>
                </Col>
              </Row>
            </Container>
          </Jumbotron>
        </Row>
        <Row />
      </Container>
    );
  }

  render_my_divs() {
    return (
      <div class="jumbotron jumbotron-fluid bg-info">
        <div class="container">
          <h1 class="display-4">Fluid jumbotron</h1>
          <p class="lead">
            This is a modified jumbotron that occupies the entire horizontal
            space of its parent.
          </p>
          <div class="card col-md-6">
            <div class="card-body">
              <ul class="nav nav-tabs" id="myTab" role="tablist">
                <li class="nav-item">
                  <a
                    class="nav-link active bg-primary"
                    id="home-tab"
                    data-toggle="tab"
                    href="#home"
                    role="tab"
                    aria-controls="home"
                    aria-selected="true"
                  >
                    Flights
                  </a>
                </li>
              </ul>
              <div class="tab-content " id="myTabContent">
                <div
                  class="tab-pane fade show active"
                  id="home"
                  role="tabpanel"
                  aria-labelledby="home-tab"
                >
                  <form class="text-left">
                    <div class="form-row">
                      <div class="form-group col-md-6">
                        <div>
                          <label for="fly-from">From:</label>
                          <input
                            class="form-control"
                            id="fly-from"
                            name="flyFrom"
                          />
                        </div>
                      </div>
                      <div class="form-group col-md-6">
                        <div>
                          <label for="fly-to">To:</label>
                          <input
                            class="form-control"
                            id="fly-to"
                            name="flyTo"
                          />
                        </div>
                      </div>
                    </div>
                    <div class="form-row">
                      <div class="form-group col-md-6">
                        <div>
                          <label for="date-from">Earliest Depart:</label>
                          <br />
                          <DatePicker
                            className="form-control"
                            selected={this.state.startDate}
                            onChange={this.handleInputChange}
                            id="date-from"
                            name="dtFrom"
                            placeholder="mm/dd/yyyy"
                          />
                        </div>
                      </div>
                      <div class="form-group col-md-6">
                        <div>
                          <label for="date-end">Latest Return:</label>
                          <br />
                          <DatePicker
                            className="form-control"
                            selected={this.state.endDate}
                            onChange={this.handleInputChange}
                            id="date-end"
                            name="dtEnd"
                            placeholder="mm/dd/yyyy"
                          />
                        </div>
                      </div>
                    </div>
                    <button type="submit" class="btn btn-primary">
                      Search
                    </button>
                  </form>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }

  render_theres() {
    return (
      <div class="fh5co-hero">
        <div class="fh5co-overlay" />
        <div
          class="fh5co-cover"
          data-stellar-background-ratio="0.2"
          styles="background-image: url(images/cover_bg_1.jpg);"
        >
          <div class="desc">
            <div class="container">
              <div class="row">
                <div class="col-sm-5 col-md-5">
                  <div class="tabulation animate-box">
                    <ul class="nav nav-tabs" role="tablist">
                      <li role="presentation" class="active">
                        <a
                          href="#flights"
                          aria-controls="flights"
                          role="tab"
                          data-toggle="tab"
                        >
                          Flights
                        </a>
                      </li>
                      <li role="presentation">
                        <a
                          href="#hotels"
                          aria-controls="hotels"
                          role="tab"
                          data-toggle="tab"
                        >
                          Hotels
                        </a>
                      </li>
                      <li role="presentation">
                        <a
                          href="#packages"
                          aria-controls="packages"
                          role="tab"
                          data-toggle="tab"
                        >
                          Packages
                        </a>
                      </li>
                    </ul>

                    <div class="tab-content">
                      <div role="tabpanel" class="tab-pane active" id="flights">
                        <div class="row">
                          <div class="col-xxs-12 col-xs-6 mt">
                            <div class="input-field">
                              <label for="from">From:</label>
                              <input
                                type="text"
                                class="form-control"
                                id="from-place"
                                placeholder="Los Angeles, USA"
                              />
                            </div>
                          </div>
                          <div class="col-xxs-12 col-xs-6 mt">
                            <div class="input-field">
                              <label for="from">To:</label>
                              <input
                                type="text"
                                class="form-control"
                                id="to-place"
                                placeholder="Tokyo, Japan"
                              />
                            </div>
                          </div>
                          <div class="col-xxs-12 col-xs-6 mt alternate">
                            <div class="input-field">
                              <label for="date-start">Check In:</label>
                              <input
                                type="text"
                                class="form-control"
                                id="date-start"
                                placeholder="mm/dd/yyyy"
                              />
                            </div>
                          </div>
                          <div class="col-xxs-12 col-xs-6 mt alternate">
                            <div class="input-field">
                              <label for="date-end">Check Out:</label>
                              <input
                                type="text"
                                class="form-control"
                                id="date-end"
                                placeholder="mm/dd/yyyy"
                              />
                            </div>
                          </div>
                          <div class="col-xxs-12 col-xs-6 mt">
                            <div class="input-field">
                              <label for="from">Excludes:</label>
                              <input
                                type="text"
                                class="form-control"
                                id="from-place"
                                placeholder=""
                              />
                            </div>
                          </div>
                          <div class="col-xs-12">
                            <input
                              type="submit"
                              class="btn btn-primary btn-block"
                              value="Search Flight"
                            />
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>
                  <div class="desc2 animate-box">
                    <div class="col-sm-7 col-sm-push-1 col-md-7 col-md-push-1">
                      <p>
                        HandCrafted by{" "}
                        <a
                          href="http://frehtml5.co/"
                          target="_blank"
                          class="fh5co-site-name"
                        >
                          FreeHTML5.co
                        </a>
                      </p>
                      <h2>Exclusive Limited Time Offer</h2>
                      <h3>Fly to Hong Kong via Los Angeles, USA</h3>
                      <span class="price">$599</span>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }
}

export default EntryForm;
