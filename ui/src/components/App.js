import React, { Component } from "react";
import { Route } from "react-router-dom";
import logo from "./logo.svg";
import "./App.css";
//import Header from "./Header";
import EntryForm from "./EntryForm";
import DisplayResults from "./DisplayResults";
import Button from "react-bootstrap/Button";
import Jumbotron from "react-bootstrap/Jumbotron";
import Form from "react-bootstrap/Form";
import Container from "react-bootstrap/Container";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Card from "react-bootstrap/Card";

class App extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      result: null,
      onChange: this.handleChange
    };
  }

  handleChange(output) {
    console.log("here");
    console.log(output);
    this.setState({ result: output });
  }

  render() {
    return (
      <div className="App">
        <EntryForm
          result={this.state.result}
          callback={v => this.handleChange(v)}
        />
        <DisplayResults result={this.state.result} />
      </div>
    );
  }
}

export default App;

/*
 result: [
        {
          flyFrom: "NYC",
          flyTo: "ABC",
          price: 0.0,
          airline: null,
          duration: null,
          arrivalTimeUTC: "0001-01-01T00:00:00",
          departTimeUTC: "0001-01-01T00:00:00",
          flightNum: null
        },
        {
          flyFrom: "ABC",
          flyTo: "NYC",
          price: 0.0,
          airline: null,
          duration: null,
          arrivalTimeUTC: "0001-01-01T00:00:00",
          departTimeUTC: "0001-01-01T00:00:00",
          flightNum: null
        }
      ],
      */
