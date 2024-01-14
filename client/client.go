package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type HTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

type Client struct {
	address string
	client  HTTPClient
}

func NewClient(c HTTPClient, address string) Client {
	return Client{
		client:  c,
		address: address,
	}
}

func (c *Client) SetAddress(address string) {
	c.address = address
}

func (c *Client) Read(key string) (string, error) {
	var body bytes.Buffer
	req := map[string]any{
		"key": key,
	}
	if err := json.NewEncoder(&body).Encode(req); err != nil {
		return "", err
	}

	httpreq, err := http.NewRequest(http.MethodGet, c.address+"/read", &body)
	if err != nil {
		return "", err
	}
	httpresp, err := c.client.Do(httpreq)
	if err != nil {
		return "", err
	}
	defer httpresp.Body.Close()

	if httpresp.StatusCode != http.StatusOK {
		buf, err := io.ReadAll(httpresp.Body)
		errtext := string(buf)
		if err != nil {
			errtext = fmt.Sprintf("an error occurred reading the body: %s", err.Error())
		}
		return "", fmt.Errorf("read failed with code %v: %s", httpresp.StatusCode, errtext)
	}

	var resp map[string]any
	if err := json.NewDecoder(httpresp.Body).Decode(&resp); err != nil {
		return "", err
	}

	return resp["value"].(string), nil
}

func (c *Client) Write(key, value string) error {
	var body bytes.Buffer
	req := map[string]any{
		"key":   key,
		"value": value,
	}
	if err := json.NewEncoder(&body).Encode(req); err != nil {
		return err
	}

	httpreq, err := http.NewRequest(http.MethodPut, c.address+"/write", &body)
	if err != nil {
		return err
	}
	httpresp, err := c.client.Do(httpreq)
	if err != nil {
		return err
	}
	defer httpresp.Body.Close()

	if httpresp.StatusCode < 200 || httpresp.StatusCode >= 300 {
		buf, err := io.ReadAll(httpresp.Body)
		errtext := string(buf)
		if err != nil {
			errtext = fmt.Sprintf("an error occurred reading the body: %s", err.Error())
		}
		return fmt.Errorf("write failed with code %v: %s", httpresp.StatusCode, errtext)
	}

	// TODO; Use response if needed.
	return nil
}
