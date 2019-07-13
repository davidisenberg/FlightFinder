import React, { Component } from "react";
import "./App.css";
import Container from "react-bootstrap/Container";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import Spinner from "react-bootstrap/Spinner";

class DisplayResults extends React.Component {
  render() {
    if (!this.props.result) return <p />;

    if (this.props.result.includes("loading")) {
      return (
        <Container>
          <Spinner animation="border" role="status">
            <span className="sr-only">Loading...</span>
          </Spinner>
        </Container>
      );
    }

    if (this.props.result.includes("exception"))
      return <p>Oops, server is down, please try again later.</p>;

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
