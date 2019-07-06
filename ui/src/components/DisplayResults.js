import React, { Component } from "react";
import "./App.css";
import Button from "react-bootstrap/Button";
import Jumbotron from "react-bootstrap/Jumbotron";
import Form from "react-bootstrap/Form";
import Container from "react-bootstrap/Container";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";

class DisplayResults extends React.Component {
  render() {
    console.log("what is this: " + this.props.result);
    if (!this.props.result) return <p />;

    if (this.props.result.includes("error")) return <p>No paths found.</p>;

    return (
      <Container>
        {this.props.result.map(flights =>
          flights === null ? (
            ""
          ) : (
            <Container>
              <Card>
                <Card.Header style={{ textAlign: "left" }}>Flights</Card.Header>
                <Table>
                  <thead>
                    <tr>
                      <th>From</th>
                      <th>To</th>
                      <th>Price</th>
                      <th>Departs</th>
                      <th>Arrives</th>
                      <th>Airline</th>
                    </tr>
                  </thead>
                  <tbody>
                    {flights.map(flight =>
                      flight === null ? (
                        ""
                      ) : (
                        <tr>
                          <td> {flight.FlyFrom}</td>
                          <td> {flight.FlyTo} </td>
                          <td> ${flight.Price} </td>
                          <td> {flight.DepartTimeUTC} </td>
                          <td> {flight.ArrivalTimeUTC} </td>
                          <td> {flight.Airline} </td>
                        </tr>
                      )
                    )}
                  </tbody>
                </Table>
              </Card>
              <br />
            </Container>
          )
        )}
      </Container>
    );
  }

  render_old() {
    console.log("what is this: " + this.props.result);
    if (!this.props.result) return <p>Loading</p>;

    return (
      <div>
        {this.props.result.map(flights =>
          flights === null ? (
            ""
          ) : (
            <table border="1">
              <tbody>
                {flights.map(flight =>
                  flight === null ? (
                    ""
                  ) : (
                    <tr>
                      <td> {flight.FlyFrom}</td>
                      <td> {flight.FlyTo} </td>
                      <td> ${flight.Price} </td>
                      <td> {flight.DepartTimeUTC} </td>
                      <td> {flight.ArrivalTimeUTC} </td>
                      <td> {flight.Airline} </td>
                    </tr>
                  )
                )}
              </tbody>
            </table>
          )
        )}
      </div>
    );
  }
}

export default DisplayResults;
