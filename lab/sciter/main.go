package main

import (
	"github.com/sciter-sdk/go-sciter"
	"github.com/sciter-sdk/go-sciter/window"
	"github.com/sirupsen/logrus"
)

func main() {
	// create window
	w, err := window.New(sciter.DefaultWindowCreateFlag, sciter.DefaultRect)
	if err != nil {
		logrus.Fatal(err)
	}
	logrus.Printf("Sciter Version: %X %X\n", sciter.Version(true), sciter.Version(false))
	// resource packing
	//rice.HandleDataLoad(w.Sciter)
	// enable debug
	ok := w.SetOption(sciter.SCITER_SET_DEBUG_MODE, 1)
	if !ok {
		logrus.Println("set debug mode failed")
	}
	// load file
	e := w.LoadHtml("<html></html>", "/")
	if e != nil {
		logrus.Println(e)
		return
	}
	root, err := w.GetRootElement()
	if err != nil {
		logrus.Panic(err)
	}
	err = root.Load("http://stockpage.10jqka.com.cn/HQ_v4.html#hs_000001", sciter.RT_DATA_HTML)
	if err != nil {
		logrus.Panic(err)
	}
	root.AttachEventHandler(&sciter.EventHandler{
		OnDataArrived: func(he *sciter.Element, params *sciter.DataArrivedParams) bool {
			logrus.Println("uri:", params.Uri(), len(params.Data()))
			return false
		},
	})
	// set handlers
	setCallbackHandlers(w)
	w.Show()
	w.Run()
}

func setCallbackHandlers(w *window.Window) {
	h := &sciter.CallbackHandler{
		OnDataLoaded: func(ld *sciter.ScnDataLoaded) int {
			logrus.Println("loaded", ld.Uri())
			return sciter.LOAD_OK
		},
		OnLoadData: func(ld *sciter.ScnLoadData) int {
			logrus.Println("loading", ld.Uri())
			return sciter.LOAD_OK
		},
	}
	w.SetCallback(h)
}
